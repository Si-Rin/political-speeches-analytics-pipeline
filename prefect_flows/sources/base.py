"""
Source adapter interface for Bronze ingestion.

A "source" is anything that can produce a list of candidate raw files to ingest (video/audio/text). 
Adding a new source later (RSS feed, YouTube channel, web scraper, API...) means writing one small class here — the ingestion flow itself (flows/ingest_bronze.py) never has to change.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Optional

@dataclass
class Candidate:
    """One raw file discovered by a source, not yet ingested/downloaded."""
    source_url: str
    source_type: str
    file_name: str
    is_local: bool = False
    mime_type: Optional[str] = None
    raw_metadata: Optional[dict] = None  # for RSS feed metadata, YouTube video metadata, ...
    local_path: Optional[str] = None  # temporary local path for sources that download files during discovery


class BaseSource(ABC):
    """Subclassed for each new type of speech source"""

    @abstractmethod
    def discover(self) -> Iterator[Candidate]:
        """
        Yield candidates to be ingested.
        Must only list/enumerate — never download or read file content here
        (that happens later, and only for candidates that pass dedup checks).
        """
        raise NotImplementedError
