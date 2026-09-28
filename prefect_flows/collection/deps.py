"""
collaborators (dependencies) of a run.
Built once per run and handed to sources through the registry.
"""
import os
from dataclasses import dataclass
from typing import Any, Optional

from prefect_flows.collection.fetcher import PoliteFetcher
from prefect_flows.collection.recorder import DiscoveryRecorder
from prefect_flows.collection.workarea import WorkArea


@dataclass
class Deps:
    """
    - work : for temporary files with max size (bytes) per run
    - fetcher : polite HTTP client with optional max bytes per request
    - recorder : records every decision (discovery, ingestion, dedup, etc.)
    - store : object store for documents (MinIO)
    - repo : Postgres for runs, bronze.documents, known-urls
    - known urls : urls already seen by the source (the repo is not exposed to sources)
    """
    work: WorkArea
    fetcher: PoliteFetcher
    recorder: DiscoveryRecorder
    store: Any
    repo: Any
    known_urls: Optional[Any] = None

    def __post_init__(self):
        self.known_urls = self.known_urls or self.repo


def build_default_deps(run_id: str) -> Deps:
    """Production wiring: Postgres + MinIO + politeness settings from the environment."""
    from prefect_flows.collection.repo import MinioStore, PostgresRepo # made the imports local to avoid circular import issues
    repo = PostgresRepo()
    max_bytes = os.environ.get("MAX_DOWNLOAD_BYTES")
    return Deps(
        work=WorkArea(run_id, max_bytes=int(os.environ["WORK_MAX_BYTES"]) if os.environ.get("WORK_MAX_BYTES") else None),
        fetcher=PoliteFetcher(
            delay_seconds=float(os.environ.get("FETCH_DELAY_SECONDS", "1.0")),
            max_bytes=int(max_bytes) if max_bytes else None,
        ),
        recorder=DiscoveryRecorder(run_id, sink=repo),
        store=MinioStore(),
        repo=repo,
    )
