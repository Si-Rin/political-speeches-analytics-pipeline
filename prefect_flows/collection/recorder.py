"""Discovery log: one row per URL decision, so a researcher can see why something was or was not collected."""
from collections import Counter
from dataclasses import dataclass, field
from typing import List, Optional, Protocol

from prefect_flows.collection.enums import CandidateStatus, MatchScope, ReasonCode


@dataclass
class DiscoveryEntry:
    source_name: str
    source_url: str
    canonical_url: str
    status: CandidateStatus
    listing_url: Optional[str] = None
    title_hint: Optional[str] = None
    reason: Optional[ReasonCode] = None
    detail: Optional[str] = None
    matched_keywords: Optional[list] = None
    match_scope: Optional[MatchScope] = None
    keywords_hash: Optional[str] = None
    doc_id: Optional[int] = None


class DiscoverySink(Protocol):
    def write_discovery(self, run_id: str, entry: DiscoveryEntry) -> None: ...


class DiscoveryRecorder:
    """Bound to one run; counts outcomes so the run can be finalized without asking the database."""

    def __init__(self, run_id: str, sink: Optional[DiscoverySink] = None):
        self.run_id = str(run_id)
        self.sink = sink
        self.counts: Counter = Counter()
        self.entries: List[DiscoveryEntry] = []      # kept in memory for tests/dry-run

    def record(self, entry: DiscoveryEntry) -> None:
        self.counts[entry.status] += 1
        self.entries.append(entry)
        if self.sink is not None:
            self.sink.write_discovery(self.run_id, entry)

    @property
    def error_count(self) -> int:
        return self.counts[CandidateStatus.FETCH_FAILED] + self.counts[CandidateStatus.INGEST_FAILED]

    def summary(self) -> dict:
        return {status.value: n for status, n in self.counts.items()}
