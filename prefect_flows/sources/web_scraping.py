"""
Source adapter: web pages discovered via keyword-based crawling.

The source starts from seed URL(s) and crawls outward through links (unlike UrlListSource), scoring each page's relevance by keyword match.
Only pages that clear the relevance bar are yielded as candidates — the crawl itself explores wider than what gets ingested.

Like UrlListSource, discover() only *lists* candidates (source_url = the page URL itself); it does not download page content.
checksum_and_stage in the ingestion flow does the actual GET + streaming + hashing later, exactly like it does for direct URL candidates.
This adapter fetches pages too, but only to read links/text for crawl decisions — that fetch is throwaway and separate from the one Bronze staging will do.

Bronze stores raw HTML as-is (source_type="text", mime_type="text/html").
Main-content extraction (stripping nav/ads/boilerplate) is a Silver-layer concern, same as Whisper transcription is for audio/video.
"""
import hashlib
import queue
import time
from collections import deque
from typing import Iterator, List, Optional, Set
import re
from urllib.parse import urljoin, urlparse, parse_qs
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

from prefect_flows.sources.base import BaseSource, Candidate

DEFAULT_USER_AGENT = "PoliticalSpeechesBot/1.0 (+research project; contact: grirasirin@gmail.com)"      # Created a custom user agent to avoid being blocked by websites

class WebCrawlSource(BaseSource):
    def __init__(
        self,
        seed_urls: List[str],
        keywords: List[str],
        allowed_domains: Optional[List[str]] = None,
        max_depth: int = 2,
        max_pages: int = 50,
        keyword_threshold: int = 1,
        crawl_delay: float = 1.0,
        user_agent: str = DEFAULT_USER_AGENT,
        request_timeout: int = 15,
    ):
        """
        seed_urls: starting point(s) for the crawl
        keywords: case-insensitive terms to match against page title/text/URL; a page is only yielded once >= keyword_threshold terms match
        allowed_domains: if set, crawl never leaves these domains (recommended — otherwise a crawl can wander off-site indefinitely)
        max_depth: how many link-hops from a seed URL to follow
        max_pages: hard cap on yielded candidates, regardless of depth
        keyword_threshold: minimum distinct keyword matches required to yield a page
        crawl_delay: seconds to sleep between requests (politeness)
        user_agent: string to send in the User-Agent header (some sites block default Python requests)
        request_timeout: seconds to wait for a response before giving up on a page
        _robots_cache: internal cache of allowed/disallowed URLs per domain (RobotFileParser instances), to avoid repeated network requests for robots.txt
        """
        self.seed_urls = seed_urls
        self.keywords = [k.lower() for k in keywords]
        self.allowed_domains = set(allowed_domains) if allowed_domains else None
        self.max_depth = max_depth
        self.max_pages = max_pages
        self.keyword_threshold = keyword_threshold
        self.crawl_delay = crawl_delay
        self.user_agent = user_agent
        self.request_timeout = request_timeout
        self._robots_cache: dict[str, RobotFileParser] = {}

    def _domain_allowed(self, url: str) -> bool:
        parsed = urlparse(url)
        domain = parsed.netloc
        return self.allowed_domains is None or domain in self.allowed_domains

    def _robots_allowed(self, url: str) -> bool:
        """
        Verifies the robots.txt rules for the given URL and user agent. Caches RobotFileParser instances per domain to avoid repeated network requests.
        RobotFileParser is a class that parses the robots.txt file to determine if the user agent is allowed to fetch the URL.
        """
        parsed = urlparse(url)
        domain = parsed.netloc
        if domain not in self._robots_cache:
            rp = RobotFileParser()
            rp.set_url(f"{parsed.scheme}://{domain}/robots.txt")
            try:
                rp.read()
            except Exception:
                # if robots.txt is unreachable, default to allow — to not block the whole crawl
                pass
            self._robots_cache[domain] = rp
        # if domain is the user agent is blocked by robots.txt, return False; otherwise, return True
        return self._robots_cache[domain].can_fetch(self.user_agent, url)

    def _make_filename(self, url: str) -> str:
        parsed = urlparse(url)
        slug = parsed.path.strip("/").replace("/", "_") or "index"
        url_hash = hashlib.md5(url.encode("utf-8")).hexdigest()[:8]
        return f"{slug[:60]}_{url_hash}.html"

    def _is_pagination_link(self, from_url: str, to_url: str) -> bool:
        """True if to_url is likely a pagination link from from_url
        Handles two patterns seen across sources:
          - query-string style: ?page=N on the same path (as UCSB)
          - path style: /page/N/ appended to the same base path (WordPress-based sites like whitehouse.gov)
        Pagination links are followed regardless of max_depth — they're traversal, not content, and don't get yielded as candidates
        """
        from_parsed, to_parsed = urlparse(from_url), urlparse(to_url)
        if from_parsed.netloc != to_parsed.netloc:          # netloc returns the domain name and port (if any) of the URL, so if they differ, it's not a pagination link
            return False
        if "page" in parse_qs(to_parsed.query):                  # if the query string contains a "page" parameter, it's likely a pagination link
            return from_parsed.path == to_parsed.path
        if re.search(r"/page/\d+/?$", to_parsed.path):          # if the path ends with /page/N/
            from_base_path = re.sub(r"/page/\d+/?$", "", to_parsed.path).rstrip("/")  # strip trailing slash for comparison
            to_base_path = re.sub(r"/page/\d+/?$", "", from_parsed.path).rstrip("/")  # strip trailing slash for comparison
            return from_base_path == to_base_path
        return False

    def discover(self) -> Iterator[Candidate]:
        visited: Set[str] = set()
        queue = deque((url, 0, None) for url in self.seed_urls)     # queue holds tuples of (url, depth, parent_url) to track crawl depth and origin
        yielded = 0
        headers = {"User-Agent": self.user_agent}

        while queue and yielded < self.max_pages:
            url, depth, parent = queue.popleft()
            if url in visited:
                continue
            visited.add(url)

            if not self._domain_allowed(url):
                continue
            if not self._robots_allowed(url):
                print(f"[WebCrawlSource] Blocked by robots.txt: {url}")
                continue

            try:
                resp = requests.get(url, headers=headers, timeout=self.request_timeout)
                resp.raise_for_status()
            except Exception as e:
                print(f"[WebCrawlSource] Failed to fetch '{url}': {e}")
                continue
            finally:
                time.sleep(self.crawl_delay)

            content_type = resp.headers.get("Content-Type", "")
            if "text/html" not in content_type:
                continue  # skip PDFs/images/.. found via links — not this source's job

            soup = BeautifulSoup(resp.text, "html.parser")  # parse the HTML content into a BeautifulSoup object
            title = soup.title.get_text(strip=True) if soup.title else ""
            page_text = soup.get_text(separator=" ", strip=True)
            haystack = f"{title} {page_text} {url}".lower() # combine title, text, and URL for keyword matching (haystack means the text to search through to find the keywords — the needle)

            matched = [kw for kw in self.keywords if kw in haystack]
            if len(matched) >= self.keyword_threshold:
                yield Candidate(
                    source_url=url,
                    source_type="text",
                    file_name=self._make_filename(url),
                    is_local=False,
                    mime_type="text/html",
                    raw_metadata={
                        "discovered_via": parent,
                        "crawl_depth": depth,
                        "matched_keywords": matched,
                        "page_title": title,
                    },
                )
                yielded += 1

            if depth < self.max_depth:
                for a in soup.find_all("a", href=True):
                    next_url = urljoin(url, a["href"]).split("#")[0]  # strip fragments (extract the base URL for crawling)
                    if next_url in visited or urlparse(next_url).scheme not in ("http", "https"):
                        continue
                    if self._is_pagination_link(url, next_url):
                        queue.append((next_url, depth, url))
                    elif depth < self.max_depth:
                        queue.append((next_url, depth + 1, url))