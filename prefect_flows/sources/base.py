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

from prefect_flows.collection.enums import CandidateStatus
from prefect_flows.collection.handles import ContentHandle
from prefect_flows.collection.recorder import DiscoveryEntry
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


class KnownUrlGate:
    """
    Lets a source skip URLs Bronze already holds WHILE DISCOVERING, so a "max N" limit counts only NEW documents:
    `if gate.skip(url): continue` placed before the source increments its counter. The skip is written to the discovery log
    (known_url_skipped) so the run still explains what it saw. With no port (tests, ad-hoc use) nothing is ever skipped.
    The pipeline keeps its own URL check as a safety net for sources that do not use the gate.
    """

    def __init__(self, known_urls=None, recorder=None, source_name: str = ""):
        self.known_urls, self.recorder, self.source_name = known_urls, recorder, source_name

    def skip(self, url: str, *, title: Optional[str] = None, listing_url: Optional[str] = None) -> bool:
        if self.known_urls is None or not url.lower().startswith(("http://", "https://")):
            return False
        canonical = canonicalize_url(url)
        if not self.known_urls.known_url(canonical):
            return False
        if self.recorder is not None:
            self.recorder.record(DiscoveryEntry(
                source_name=self.source_name, source_url=url, canonical_url=canonical, status=CandidateStatus.KNOWN_URL_SKIPPED,
                title_hint=title, listing_url=listing_url,
                detail="Already in Bronze; skipped (does not count toward the limit)."))
        return True
