"""
Bronze ingestion flow: source-agnostic. It resolves an adapter through the registry, consumes its candidate stream
one at a time (materialize -> SHA-256 -> dedup -> MinIO -> bronze.documents) and records every decision.

Run it through Prefect (deployment) or from the command line: see prefect_flows/cli.py.
"""
import sys
import uuid
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from prefect import flow, get_run_logger
from prefect.runtime import flow_run

sys.path.append(str(Path(__file__).resolve().parent.parent))
from prefect_flows.collection.deps import build_default_deps
from prefect_flows.collection.enums import CandidateStatus 
from prefect_flows.collection.runner import run_collection 

load_dotenv()


def _process_downstream(doc_ids: list, logger) -> dict:
    """Run Silver (download from MinIO -> extract/transcribe) then the per-document Gold flows on the new documents.
    Topics are corpus-level (BERTopic fits over the docs it is given), so run `gold_topics` separately on the full corpus.
    Failures here never fail the Bronze run: Bronze is already committed and Silver/Gold can be re-run for pending docs."""
    result = {}
    stages = (
        ("silver", "prefect_flows.silver_ingest", "ingest_silver"),
        ("gold_sentiment", "prefect_flows.gold_sentiment", "ingest_gold_sentiment"),
        ("gold_linguistic", "prefect_flows.gold_linguistic", "ingest_gold_linguistic"),
        ("gold_labels", "prefect_flows.gold_labels", "ingest_gold_labels"),
    )
    import importlib
    for name, module, fn in stages:
        try:
            result[name] = getattr(importlib.import_module(module), fn)(doc_ids=doc_ids)
        except Exception as exc:
            logger.error(f"{name} failed for doc_ids={doc_ids}: {type(exc).__name__}: {exc}")
            result[name] = {"error": str(exc)}
            if name == "silver":                      # Gold needs Silver text
                break
    return result


@flow(name="bronze-ingestion")
def ingest_bronze(source_name: str, run_id: Optional[str] = None, params: Optional[dict] = None,
                  params_version: int = 1, dry_run: bool = False, process_downstream: bool = True):
    """Contract with the backend: ingest_bronze(run_id, source_name, params, params_version, dry_run).
    process_downstream (default True, optional for the backend) chains Silver + Gold on the documents this run ingested."""
    logger = get_run_logger()
    params = params or {}
    run_id = run_id or str(uuid.uuid4())
    deps = build_default_deps(run_id)
    summary = run_collection(run_id, source_name, params, deps, dry_run=dry_run,
                             params_version=params_version, prefect_flow_run_id=flow_run.id, logger=logger)
    doc_ids = [e.doc_id for e in deps.recorder.entries if e.status == CandidateStatus.INGESTED and e.doc_id]
    if process_downstream and not dry_run and doc_ids:
        summary = {**summary, "downstream": _process_downstream(doc_ids, logger)}
    return summary