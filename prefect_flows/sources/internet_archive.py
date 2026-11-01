"""
Source adapter: Internet Archive TV News Archive items (containing Trump appearances)

Each item page represents one broadcast, split into ~1-minute segments (data-idx). Some segments aren't Trump's own speech (a moderator's intro, applause-only music)
This adapter records per-segment timing at discovery time
   
    — "excluded_indices" marks which segments to skip

Bronze stores the RAW, untrimmed audio, per this project's philosophy — segment exclusion/trimming happens in Silver's InternetArchiveExtractor, using the timing metadata captured here
"""
import json
import re
from typing import Dict, Iterator, List, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from prefect_flows.extractors.utils import parse_date
from prefect_flows.sources.base import BaseSource, Candidate

DEFAULT_USER_AGENT = "PoliticalSpeechesBot/1.0 (+research project; contact: you@example.com)"


class InternetArchiveSource(BaseSource):
    def __init__(
        self,
        urls: List[str],
        excluded_indices: Optional[Dict[str, List[int]]] = None,
        request_timeout: int = 30,
    ):
        """
        - urls: Internet Archive TV News item detail page URLs
        - excluded_indices: identifier -> list of data-idx values to skip, keyed by identifier (not URL) since that's the stable id parsed from the embedded JSON, robust to /start/X/end/Y query variations in the URL
        """
        self.urls = urls
        self.excluded_indices = excluded_indices or {}
        self.request_timeout = request_timeout
        self.headers = {"User-Agent": DEFAULT_USER_AGENT}

    def _fetch(self, url: str) -> Optional[BeautifulSoup]:
        try:
            resp = requests.get(url, headers=self.headers, timeout=self.request_timeout)
            resp.raise_for_status()
        except Exception as e:
            print(f"[InternetArchiveSource] Failed to fetch '{url}': {e}")
            return None
        return BeautifulSoup(resp.text, "html.parser")

    def _parse_tv3_init(self, soup: BeautifulSoup) -> Optional[dict]:
        tag = soup.find("input", class_="js-tv3-init")
        if not tag or not tag.get("value"):
            return None
        try:
            return json.loads(tag["value"])
        except json.JSONDecodeError as e:
            print(f"[InternetArchiveSource] Failed to parse TV3 init JSON: {e}")
            return None

    def _parse_segments(self, soup: BeautifulSoup) -> List[dict]:
        segments = []
        for col in soup.find_all("div", class_="tvcol"):
            idx = col.get("data-idx")
            if idx is None:
                continue
            idx = int(idx)

            link = col.find("a", class_="js-tv2-col_clicked", href=True)
            start, end = None, None
            if link:
                m = re.search(r"/start/(\d+)/end/(\d+)", link["href"])
                if m:
                    start, end = int(m.group(1)), int(m.group(2))

            snippet = col.find("div", class_="snipin")
            segments.append({
                "idx": idx,
                "start_sec": start,
                "end_sec": end,
                "caption_text": snippet.get_text(strip=True) if snippet else "",
            })
        return sorted(segments, key=lambda s: s["idx"])

    def _extract_title(self, soup: BeautifulSoup) -> Optional[str]:
        h1 = soup.find("h1", class_="item-title")
        return h1.get_text(separator=" ", strip=True) if h1 else None

    def _extract_date(self, soup: str) -> Optional[str]:
        date_container = soup.find("div", class_="tv-ttl").find("div")
        match = re.search(r"([A-Za-z]+ \d{1,2}, \d{4})", date_container.text)
        extracted_date = match.group(1) if match else None
        return parse_date(extracted_date)

    def discover(self) -> Iterator[Candidate]:
        for url in self.urls:
            soup = self._fetch(url)
            if soup is None:
                continue

            tv3 = self._parse_tv3_init(soup)
            if not tv3 or not tv3.get("TV3.identifier"):
                print(f"[InternetArchiveSource] No identifier found on '{url}', skipping")
                continue
            identifier = tv3["TV3.identifier"]

            yield Candidate(
                source_url=urljoin(url, f"/download/{identifier}/{identifier}.mp3"),
                source_type="audio",
                file_name=f"{identifier}.mp3",
                is_local=False,
                mime_type="audio/mpeg",
                raw_metadata={
                    "source_name": "internet_archive",
                    "title": self._extract_title(soup),
                    "publication_date": self._extract_date(soup),
                    "verified_content": None,
                    "content_source": None,
                    "extra": {
                        "identifier": identifier,
                        "page_url": url,
                        "segments": self._parse_segments(soup),
                        "excluded_indices": self.excluded_indices.get(identifier, []),
                    },
                },
            )