"""
Consuming one Candidate: materialize -> hash -> dedup -> upload -> register -> record.

Plain functions over ports (ObjectStore, Bronze repo, recorder), testable without Prefect, MinIO or Postgres.
"""
import hashlib
from pathlib import Path
from typing import Optional

from prefect_flows.collection.enums import CandidateStatus, ReasonCode
from prefect_flows.collection.fetcher import FetchError
from prefect_flows.collection.handles import MaterializeContext
from prefect_flows.collection.recorder import DiscoveryEntry, DiscoveryRecorder
from prefect_flows.collection.workarea import WorkArea
from prefect_flows.sources.base import Candidate

CHUNK_SIZE = 8 * 1024 * 1024


def sha256_of(path: Path):
    digest, size = hashlib.sha256(), 0
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            digest.update(chunk)
            size += len(chunk)
    return digest.hexdigest(), size


def _entry(c: Candidate, source_name: str, status: CandidateStatus, **kw) -> DiscoveryEntry:
    meta = c.raw_metadata or {}
    coll = meta.get("collection") or {}
    return DiscoveryEntry(
        source_name=source_name, source_url=c.source_url, canonical_url=c.canonical_url, status=status,
        listing_url=c.listing_url, title_hint=c.title_hint,
        matched_keywords=coll.get("matched_keywords"), keywords_hash=coll.get("keywords_hash"), **kw,
    )


def provenance(c: Candidate, source_name: str, run_id: str) -> dict:
    """raw_metadata as stored in Bronze: what the source gave us plus which source/run brought it."""
    meta = dict(c.raw_metadata or {})
    meta.setdefault("source_name", source_name)
    collection = dict(meta.get("collection") or {})
    collection.update(run_id=str(run_id), canonical_url=c.canonical_url)
    meta["collection"] = collection
    return meta


def ingest_candidate(candidate: Candidate, *, source_name: str, run_id: str, work: WorkArea, fetcher, store, bronze,
                     recorder: DiscoveryRecorder, dry_run: bool = False) -> CandidateStatus:
    """Never raises for a per-candidate problem: the outcome is recorded and the run continues."""
    def record(status: CandidateStatus, **kw) -> CandidateStatus:
        recorder.record(_entry(candidate, source_name, status, **kw))
        return status

    with candidate:                                   # releases the handle on every path
        if dry_run:
            return record(CandidateStatus.PREFILTER_PASSED)
        try:
            with work.scope() as scope:               # scope for whatever this consumer downloads
                try:
                    path = candidate.handle.materialize(MaterializeContext(scope=scope, fetcher=fetcher))
                    checksum, size = sha256_of(path)
                    work.check_budget()
                except FetchError as exc:
                    return record(CandidateStatus.FETCH_FAILED, reason=exc.reason, detail=exc.detail)
                except Exception as exc:
                    return record(CandidateStatus.FETCH_FAILED, detail=f"{type(exc).__name__}: {exc}")

                if bronze.checksum_exists(checksum):
                    return record(CandidateStatus.DUPLICATE_CONTENT, reason=ReasonCode.CHECKSUM_EXISTS)

                object_key = f"{checksum}{Path(candidate.file_name).suffix}"
                try:
                    store.put(path, object_key, candidate.mime_type or "application/octet-stream")
                except Exception as exc:
                    return record(CandidateStatus.INGEST_FAILED, reason=ReasonCode.UPLOAD_FAILED, detail=str(exc))
                try:
                    doc_id: Optional[int] = bronze.insert_document(
                        source_url=candidate.source_url, source_type=candidate.source_type,
                        file_name=candidate.file_name, file_size=size, mime_type=candidate.mime_type,
                        checksum=checksum, raw_metadata=provenance(candidate, source_name, run_id),
                        storage_path=object_key, run_id=run_id)
                except Exception as exc:
                    return record(CandidateStatus.INGEST_FAILED, reason=ReasonCode.DB_ERROR, detail=str(exc))
                if doc_id is None:                    # lost a race with another writer of the same bytes
                    return record(CandidateStatus.DUPLICATE_CONTENT, reason=ReasonCode.CHECKSUM_EXISTS)
                return record(CandidateStatus.INGESTED, doc_id=doc_id)
        except Exception as exc:                      # work-area/budget/bookkeeping problems
            return record(CandidateStatus.INGEST_FAILED, detail=f"{type(exc).__name__}: {exc}")
