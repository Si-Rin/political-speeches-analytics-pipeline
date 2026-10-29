"""
Source adapter interface for Bronze ingestion.

A source produces a *stream* of Candidates (the flow consumes them one at a time). A Candidate is the only
thing that crosses the boundary between adapters and the flow: it carries a ContentHandle that knows how to get
the bytes and who cleans up. Adding a source means writing one small class and registering it in
sources/registry.py; the flow never changes.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Iterator, Optional

from prefect_flows.collection.handles import ContentHandle
from prefect_flows.collection.urls import canonicalize_url


@dataclass
class Candidate:
    """One raw file a source wants ingested. Use it as a context manager: leaving the block releases its handle."""
    source_url: str
    source_type: str                     # 'text' | 'audio' | 'video'
    file_name: str
    handle: ContentHandle
    mime_type: Optional[str] = None
    raw_metadata: Optional[dict] = None  # RSS/YouTube/probe metadata, provenance, user_provided...
    listing_url: Optional[str] = None
    title_hint: Optional[str] = None
    canonical_url: Optional[str] = None

    def __post_init__(self):
        if self.canonical_url is None:
            self.canonical_url = canonicalize_url(self.source_url)

    def __enter__(self) -> "Candidate":
        return self

    def __exit__(self, *exc) -> None:
        self.handle.release()


class BaseSource(ABC):
    """Subclassed for each new type of speech source."""

    @abstractmethod
    def candidates(self) -> Iterator[Candidate]:
        """
        Yield candidates lazily, one at a time. Ownership rule: whatever a source downloads but does not yield,
        it releases itself; whatever it yields, the consumer releases (Candidate.__exit__).
        """
        raise NotImplementedError