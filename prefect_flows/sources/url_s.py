"""
Source adapter: ingests a fixed, explicit list of direct-download URLs.
A simple starting point before a real scraper/RSS reader exists — just
supply the URLs you want ingested.
"""
import mimetypes
from typing import Iterator, List
from urllib.parse import urlparse

from prefect_flows.sources.base import BaseSource, Candidate


class UrlListSource(BaseSource):
    def __init__(self, urls: List[str], source_type: str = "video"):
        """
        urls: list of direct file-download URLs (video/audio/text)
        source_type: "video" | "audio" | "text" — assumed the same for every
                     URL in this list (use two UrlListSource instances if
                     you need to mix types in one run)
        """
        self.urls = urls
        self.source_type = source_type

    def discover(self) -> Iterator[Candidate]:
        for url in self.urls:
            file_name = _filename_from_url(url)
            mime_type, _ = mimetypes.guess_type(file_name)
            yield Candidate(
                source_url=url,
                source_type=self.source_type,
                file_name=file_name,
                is_local=False,
                mime_type=mime_type,
            )


def _filename_from_url(url: str) -> str:
    path = urlparse(url).path
    return path.rsplit("/", 1)[-1] or "unnamed_file"
