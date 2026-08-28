"""
Source adapter: Miller Center presidential speech pages.

The Miller Center hosts a collection of presidential speeches, which can be accessed via a series of linked pages. 
This source adapter allows for two modes of operation:
1. **Direct URL Mode**: If a list of specific speech URLs is provided, the adapter will fetch and process only those pages.
2. **Crawl Mode**: If a starting URL is provided, the adapter will begin at that page and follow the "next" links until the end of chain or the maximum depth is reached.
"""
from typing import Iterator, List, Optional
from urllib.parse import urljoin
import time
from typing import Set
import requests
from bs4 import BeautifulSoup

from prefect_flows.sources.base import BaseSource, Candidate

DEFAULT_USER_AGENT = "PoliticalSpeechesBot/1.0 (+research project; contact: grirasirin@gmail.com)"


class MillerCenterSource(BaseSource):
    def __init__(
        self,
        urls: Optional[List[str]] = None,
        start_url: Optional[str] = None,
        max_depth: int = 1000,
        crawl_delay: float = 1.0,
        request_timeout: int = 20,
    ):
        """
        Two mutually exclusive modes:
          - urls: fetch exactly these speech pages, no chain-following.
          - start_url: begin here and walk forward via each page's "next" link until the chain ends or max_depth is hit.
        """
        if not urls and not start_url:
            raise ValueError("Provide either urls or start_url")
        self.urls = urls
        self.start_url = start_url
        self.max_depth = max_depth
        self.crawl_delay = crawl_delay
        self.request_timeout = request_timeout
        self.headers = {"User-Agent": DEFAULT_USER_AGENT}

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.request_timeout)
            resp.raise_for_status()
        except Exception as e:
            print(f"[MillerCenterSource] Failed to fetch '{url}': {e}")
            return None
        return BeautifulSoup(resp.text, "html.parser")

    def _extract_next_url(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        next_block = soup.find("div", class_="next")
        if not next_block:
            return None
        link = next_block.find("a", href=True)
        next_url = urljoin(current_url, link["href"]) if link else None     # next link is relative (doesnt include domain : millercenter.org), so join with current_url
        print(f"[MillerCenterSource] Next URL: {next_url}")
        return next_url


    def _build_candidate(self, url: str, soup: BeautifulSoup) -> Candidate:
        return Candidate(
            source_url=url,
            source_type="text",
            file_name=url.rstrip("/").split("/")[-1] + ".html",
            is_local=False,
            mime_type="text/html",
            raw_metadata={
                "source_name": "miller_center",
            },
        )

    def discover(self) -> Iterator[Candidate]:
        if self.urls:
            for url in self.urls:
                soup = self._fetch(url)
                if soup is None:
                    continue
                yield self._build_candidate(url, soup)
                time.sleep(self.crawl_delay)
            return

        # chain mode
        current_url = self.start_url
        visited: Set[str] = set()
        count = 0

        while current_url and count < self.max_depth:
            if current_url in visited:
                print(f"[MillerCenterSource] Cycle detected at '{current_url}', stopping")
                break
            visited.add(current_url)

            soup = self._fetch(current_url)
            if soup is None:
                break  # chain broken — can't discover next_url without this page

            yield self._build_candidate(current_url, soup)
            count += 1

            next_url = self._extract_next_url(soup, current_url)
            time.sleep(self.crawl_delay)
            current_url = next_url