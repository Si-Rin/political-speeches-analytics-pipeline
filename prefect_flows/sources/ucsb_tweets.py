"""
Source adapter: archived Trump's daily tweets discovered via keyword-based crawling from UCSB

The source fetches Related Documets list and looks for archived tweets compilation pages, only pages that have a title like "Tweets of <month> <day>, <year>" are yielded as candidates — the crawl itself explores wider than what gets ingested.

Discovers tweet-compilation documents by paginating through a president's document listing page and yielding only links whose anchor text matches link_text_filter (default "tweets of")
Here, only one fetch per *listing* page is needed; matching documents are identified from the listing page's own link text, with zero extra requests

Bronze stores the raw HTML page, same as MillerCenterSource — extraction (retweet filtering, table parsing) happens in Silver via UcsbTweetsExtractor, not here
"""

import time
from typing import Iterator, Optional, Set
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from prefect_flows.sources.base import BaseSource, Candidate

DEFAULT_USER_AGENT = "PoliticalSpeechesBot/1.0 (+research project; contact: grirasirin@gmail.com)"


class UcsbTweetsSource(BaseSource):
    def __init__(
        self,
        listing_url: str,
        link_text_filter: str = "tweets of",
        max_documents: Optional[int] = 1,
        crawl_delay: float = 1.0,
        request_timeout: int = 20,
    ):
        """
        - listing_url: the president's document listing page to paginate through (https://www.presidency.ucsb.edu/people/president/donald-j-trump-1st-term & https://www.presidency.ucsb.edu/people/president/donald-j-trump-2nd-term)
        - link_text_filter: case-insensitive substring an anchor's text must contain to be yielded as a candidate
        - max_documents: cap on yielded tweet-day candidates (not on listing pages visited — the crawl may page through many listing pages before finding enough matches)
        """
        self.listing_url = listing_url
        self.link_text_filter = link_text_filter.lower()
        self.max_documents = max_documents
        self.crawl_delay = crawl_delay
        self.request_timeout = request_timeout
        self.headers = {"User-Agent": DEFAULT_USER_AGENT}
        
    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.request_timeout)
            resp.raise_for_status()
        except Exception as e:
            print(f"[UcsbTweetsSource] Failed to fetch '{url}': {e}")
            return None
        return BeautifulSoup(resp.text, "html.parser")

    def _next_listing_page(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """UCSB paginates via <li class="next"><a href="?page=N">."""
        next_li = soup.find("li", class_="next")
        if not next_li:
            return None
        link = next_li.find("a", href=True)
        return urljoin(current_url, link["href"]) if link else None

    def _build_candidate(self, url: str, link_text: str) -> Candidate:
        return Candidate(
            source_url=url,
            source_type="text",
            file_name=url.rstrip("/").split("/")[-1] + ".html",
            is_local=False,
            mime_type="text/html",
            raw_metadata={
                "source_name": "ucsb_tweets",
                "title": link_text,        # e.g. "Tweets of June 16, 2015"
                "content_source": None,
                "extra": {},
            },
        )
        
    def discover(self) -> Iterator[Candidate]:
        current_url = self.listing_url
        visited: Set[str] = set()
        yielded_urls: Set[str] = set()
        count = 0

        while current_url:
            if self.max_documents is not None and count >= self.max_documents:
                break
            if current_url in visited:
                print(f"[UcsbTweetsSource] Cycle detected at '{current_url}', stopping")
                break
            visited.add(current_url)

            soup = self._fetch(current_url)
            if soup is None:
                break  # can't find matches or the next page without this page

            for a in soup.find_all("a", href=True):
                if self.max_documents is not None and count >= self.max_documents:
                    break
                link_text = a.get_text(strip=True)
                if self.link_text_filter not in link_text.lower():
                    continue
                doc_url = urljoin(current_url, a["href"]).split("#")[0]
                if doc_url in yielded_urls:
                    continue
                yielded_urls.add(doc_url)
                yield self._build_candidate(doc_url, link_text)
                count += 1

            next_url = self._next_listing_page(soup, current_url)
            time.sleep(self.crawl_delay)
            current_url = next_url

        print(f"[UcsbTweetsSource] Finished discovery. Total tweet-day documents: {count}")