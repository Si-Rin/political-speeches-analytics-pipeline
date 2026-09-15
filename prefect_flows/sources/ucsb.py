"""
Source adapter: archived Trump social-media compilation pages from UCSB — covers both the 1st-term "Tweets of <date>" format and the 2nd-term "Truth Social Posts of <date>" format,
since Trump's primary platform changed between terms and UCSB compiles each under a different title.

Discovers documents by paginating through a president's document listing page and yielding only links whose anchor text matches one of link_text_filters.
One fetch per *listing*; matching documents are identified from the listing page's own link text, with zero extra requests.

Both formats share the single "ucsb" source_name, but they are distinguished by their pages' table row shape at parse time (extraction in silver).
Bronze stores the raw HTML page, extraction (retweet/repost filtering, table parsing) happens in Silver via the single UcsbExtractor class.
"""

import time
from typing import Iterable, Iterator, List, Optional, Set, Union
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from prefect_flows.sources.base import BaseSource, Candidate

DEFAULT_USER_AGENT = "PoliticalSpeechesBot/1.0 (+research project; contact: grirasirin@gmail.com)"

DEFAULT_LINK_TEXT_FILTERS = ("tweets of", "truth social posts of")

FILTER_TO_CONTENT_TYPE = {
    "tweets of": "tweet",
    "truth social posts of": "truth_social",
}

SOURCE_NAME = "ucsb"


class UcsbSource(BaseSource):
    def __init__(
        self,
        listing_url: str,
        link_text_filter: Union[str, Iterable[str]] = DEFAULT_LINK_TEXT_FILTERS,
        max_documents: Optional[int] = 1,
        crawl_delay: float = 1.0,
        request_timeout: int = 20,
    ):
        """
        - listing_url: the president's document listing page to paginate through (/donald-j-trump-1st-term or /donald-j-trump-2nd-term)
        - link_text_filter: an anchor's text must contain (case-insensitive) to be yielded as a candidate.
        - max_documents: cap on yielded candidates (not on listing pages visited)
        """
        self.listing_url = listing_url
        self.link_text_filters: List[str] = (
            [link_text_filter.lower()]
            if isinstance(link_text_filter, str)
            else [f.lower() for f in link_text_filter]
        )
        self.max_documents = max_documents
        self.crawl_delay = crawl_delay
        self.request_timeout = request_timeout
        self.headers = {"User-Agent": DEFAULT_USER_AGENT}

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.request_timeout)
            resp.raise_for_status()
        except Exception as e:
            print(f"[UcsbSource] Failed to fetch '{url}': {e}")
            return None
        return BeautifulSoup(resp.text, "html.parser")

    def _next_listing_page(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """UCSB paginates via <li class="next"><a href="?page=N">."""
        next_li = soup.find("li", class_="next")
        if not next_li:
            return None
        link = next_li.find("a", href=True)
        return urljoin(current_url, link["href"]) if link else None

    def _matched_content_type(self, link_text_lower: str) -> Optional[str]:
        """Returns the content_type for the first configured filter that matches this link's text, or None if none match -> skip the link."""
        for filter_text in self.link_text_filters:
            if filter_text in link_text_lower:
                return FILTER_TO_CONTENT_TYPE.get(filter_text, "unknown")
        return None

    def _build_candidate(self, url: str, link_text: str, content_type: str) -> Candidate:
        return Candidate(
            source_url=url,
            source_type="text",
            file_name=url.rstrip("/").split("/")[-1] + ".html",
            is_local=False,
            mime_type="text/html",
            raw_metadata={
                "source_name": SOURCE_NAME,
                "title": link_text,  # e.g. "Tweets of June 16, 2015" / "Truth Social Posts of April 24, 2025"
                "content_source": None,
                "extra": {"matched_format": content_type},
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
                print(f"[UcsbSource] Cycle detected at '{current_url}', stopping")
                break
            visited.add(current_url)

            soup = self._fetch(current_url)
            if soup is None:
                break  # can't find matches or the next page without this page

            for a in soup.find_all("a", href=True):
                if self.max_documents is not None and count >= self.max_documents:
                    break
                link_text = a.get_text(strip=True)
                content_type = self._matched_content_type(link_text.lower())
                if content_type is None:
                    continue
                doc_url = urljoin(current_url, a["href"]).split("#")[0]
                if doc_url in yielded_urls:
                    continue
                yielded_urls.add(doc_url)
                yield self._build_candidate(doc_url, link_text, content_type)
                count += 1

            next_url = self._next_listing_page(soup, current_url)
            time.sleep(self.crawl_delay)
            current_url = next_url

        print(f"[UcsbSource] Finished discovery. Total documents: {count}")