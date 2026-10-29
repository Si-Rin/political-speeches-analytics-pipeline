"""
Source adapter: Miller Center presidential speech pages.

The Miller Center hosts a collection of presidential speeches, which can be accessed via a series of linked pages. 
This source adapter allows for two modes of operation:
1. **Direct URL Mode**: If a list of specific speech URLs is provided, the adapter will fetch and process only those pages.
2. **Crawl Mode**: If a starting URL is provided, the adapter will begin at that page and follow the "next" links until the end of chain or the maximum depth is reached.

Either mode also accepts a LISTING page (…/presidential-speeches, with or without filter query such as ?field_president_target_id[..]=..):
the speech links it contains are collected, then its pagination is followed, up to max_depth speeches. Listing pages are never stored as documents.
"""
from bs4 import BeautifulSoup
import re
import requests
import time
from typing import Iterator, List, Optional, Set
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit


from prefect_flows.collection.handles import HttpHandle
from prefect_flows.collection.urls import safe_file_name
from prefect_flows.sources.base import BaseSource, Candidate

from prefect_flows.http_config import USER_AGENT as DEFAULT_USER_AGENT


LISTING_PATH_SUFFIX = "/presidential-speeches"
_SPEECH_PATH = re.compile(r"^/the-presidency/presidential-speeches/[^/?#]+/?$")
MAX_LISTING_PAGES = 200                    # hard stop against endless pagination


def is_listing_url(url: str) -> bool:
    """A listing page is the speeches index itself (no speech slug), whatever its filter query."""
    return urlsplit(url).path.rstrip("/").endswith(LISTING_PATH_SUFFIX)


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


    def _build_candidate(self, url: str, soup: Optional[BeautifulSoup], title: Optional[str] = None) -> Candidate:
        return Candidate(
            source_url=url,
            title_hint=title or None,
            source_type="text",
            file_name=safe_file_name(url),
            handle=HttpHandle(url, filename=safe_file_name(url)),
            mime_type="text/html",
            raw_metadata={
                "source_name": "miller_center",
            },
        )

    def _speech_links(self, soup: BeautifulSoup, page_url: str) -> List[tuple]:
        """(speech URL, title) pairs of a listing page, in page order, without query/fragment, de-duplicated.
        Scoped to the result rows (`.views-row a`) so featured / "Most Read" / sidebar / footer links are never collected;
        a page without result rows yields nothing (which ends the listing)."""
        # ONLY links inside result rows. No fallback to "every link on the page": a results page with no rows (past the last page,
        # or an empty filter) still shows the "Most Read Speeches" block with other presidents' speeches, which would be collected.
        anchors = [a for a in soup.select(".views-row a[href]") if not a.find_parent(class_="dynamic-content")]
        found, seen = [], set()
        for a in anchors:
            full = urljoin(page_url, a["href"])
            parts = urlsplit(full)
            if parts.netloc.lower().endswith("millercenter.org") and _SPEECH_PATH.match(parts.path):
                clean = urlunsplit((parts.scheme, parts.netloc, parts.path.rstrip("/"), "", ""))
                if clean not in seen:
                    seen.add(clean)
                    found.append((clean, " ".join(a.get_text().split())))
        return found

    @staticmethod
    def _merge_query(current_url: str, href: str) -> str:
        """Next-page URL keeping the current filters: Drupal's infinite-scroll pager may give a bare `?page=1`,
        and urljoin would then DROP the president filter and walk the whole archive."""
        cur, nxt = urlsplit(current_url), urlsplit(urljoin(current_url, href))
        query = dict(parse_qsl(cur.query, keep_blank_values=True))
        query.update(dict(parse_qsl(nxt.query, keep_blank_values=True)))
        return urlunsplit((nxt.scheme, nxt.netloc, nxt.path, urlencode(query), ""))

    def _next_listing_page(self, soup: BeautifulSoup, current_url: str) -> Optional[str]:
        """Drupal pager (rel=next, infinite-scroll "load more" or pager__item--next); otherwise bump ?page=N (0-based)."""
        link = soup.find("a", rel=lambda r: r and "next" in (r if isinstance(r, list) else [r]))
        if link is None:
            li = soup.find("li", class_=lambda c: c and "pager__item--next" in c)
            link = li.find("a", href=True) if li else None
        if link is None:
            pager = soup.select_one("[data-drupal-views-infinite-scroll-pager] a[href], .js-pager__items a[href]")
            link = pager
        if link is not None and link.get("href"):
            return self._merge_query(current_url, link["href"])
        parts = urlsplit(current_url)
        query = dict(parse_qsl(parts.query, keep_blank_values=True))
        query["page"] = str(int(query.get("page", "0")) + 1)
        return urlunsplit((parts.scheme, parts.netloc, parts.path, urlencode(query), ""))

    def _expand_listing(self, listing_url: str, limit: int) -> Iterator[tuple]:
        """Yield (speech URL, title) from a listing page and its following pages. stops at `limit`, when a page shows no speech link
        (end of listing), or after MAX_LISTING_PAGES. Speeches already in Bronze are skipped later by the pipeline (known_url_skipped)"""
        seen_pages, seen_speeches, page_url, pages = set(), set(), listing_url, 0
        while page_url and page_url not in seen_pages and pages < MAX_LISTING_PAGES and len(seen_speeches) < limit:
            seen_pages.add(page_url)
            soup = self._fetch(page_url)
            pages += 1
            if soup is None:
                return
            new = [(u, t) for u, t in self._speech_links(soup, page_url) if u not in seen_speeches]
            if not new:
                return                                  # no progress: end of the listing (or a layout we do not recognise)
            for u, t in new:
                if len(seen_speeches) >= limit:
                    return
                seen_speeches.add(u)
                yield u, t
            time.sleep(self.crawl_delay)
            page_url = self._next_listing_page(soup, page_url)

    def candidates(self) -> Iterator[Candidate]:
        listings = [u for u in (self.urls or [self.start_url]) if u and is_listing_url(u)]
        if listings:
            # speeches are fetched later by the pipeline (HttpHandle), so no extra request is needed here
            remaining = self.max_depth
            for listing in listings:
                for url, title in self._expand_listing(listing, remaining):
                    yield self._build_candidate(url, None, title)
                    remaining -= 1
                if remaining <= 0:
                    return
            if not self.urls:
                return
            self.urls = [u for u in self.urls if not is_listing_url(u)]
            if not self.urls:
                return
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