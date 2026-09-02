"""
Source adapter: a single, already-known item — one URL or one local file path, with its content type given explicitly by the caller (the backend's Collection UI).
Unlike LocalFolderSource (ingests every supported file in a folder) or the site-specific crawlers, this yields exactly one Candidate, no enumeration/discovery involved.

Not for YouTube URLs: those are web pages, not direct file links (a plain GET returns HTML, not media bytes) 
Route those through YoutubeSource instead, same as bronze_ingest.py already does for --source youtube.
"""
import mimetypes
from pathlib import Path
from typing import Iterator, Optional
from urllib.parse import urlparse

from prefect_flows.sources.base import BaseSource, Candidate


class SingleItemSource(BaseSource):
    def __init__(
        self,
        location: str,
        source_type: str,
        is_local: bool,
        raw_metadata: Optional[dict] = None,
    ):
        """
        location: a URL (is_local=False) or a local filesystem path (is_local=True)
        source_type: 'text' | 'video' | 'audio' — matches bronze.documents' CHECK constraint
        raw_metadata: optional, e.g. whatever the probe step detected (title, duration...)
        """
        self.location = location
        self.source_type = source_type
        self.is_local = is_local
        self.raw_metadata = raw_metadata

    def discover(self) -> Iterator[Candidate]:
        if self.is_local:
            path = Path(self.location)
            if not path.exists() or not path.is_file():
                raise FileNotFoundError(f"Local file not found: {self.location}")
            file_name = path.name
            mime_type, _ = mimetypes.guess_type(str(path))
            source_url = str(path.resolve())
        else:
            file_name = Path(urlparse(self.location).path).name or "downloaded_file"
            mime_type, _ = mimetypes.guess_type(file_name)
            source_url = self.location

        yield Candidate(
            source_url=source_url,
            source_type=self.source_type,
            file_name=file_name,
            is_local=self.is_local,
            mime_type=mime_type,
            raw_metadata=self.raw_metadata,
        )