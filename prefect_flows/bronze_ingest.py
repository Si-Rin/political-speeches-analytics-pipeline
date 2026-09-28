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
from prefect_flows.collection.deps import build_default_deps    # noqa: E402
from prefect_flows.collection.runner import run_collection      # noqa: E402

load_dotenv()


@flow(name="bronze-ingestion")
def ingest_bronze(source_name: str, run_id: Optional[str] = None, params: Optional[dict] = None,
                  params_version: int = 1, dry_run: bool = False):
    """Contract with the backend: ingest_bronze(run_id, source_name, params, params_version, dry_run)."""
    logger = get_run_logger()
    params = params or {}
    run_id = run_id or str(uuid.uuid4())
    return run_collection(run_id, source_name, params, build_default_deps(run_id), dry_run=dry_run,
                          params_version=params_version, prefect_flow_run_id=flow_run.id, logger=logger)
