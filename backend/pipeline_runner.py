"""
Triggers Bronze ingestion for one submitted item, via subprocess, never by importing prefect_flows.bronze_ingest directly.

Why subprocess and not a direct function call: bronze_ingest.py imports `from prefect import flow, task, get_run_logger` at module level, which pulls in Prefect's anyio<4.0 pin. 
This backend deliberately does NOT depend on prefect (see backend_requirements.txt) so it can live in a separate venv from prefect_flows/ without a dependency conflict — the whole reason this backend/frontend split exists. 
Importing bronze_ingest here, even just to call one function, would defeat that.

This also makes triggering naturally non-blocking: Popen returns immediately, the actual download/checksum/upload/insert happens in a separate OS process, and the caller (a FastAPI route) returns right away.
Ingestion progress is observed by polling the DB (see routes/status.py), not by waiting on this subprocess.
"""
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, Optional

# Repo root: this file lives in backend/, prefect_flows/ is a sibling dir.
REPO_ROOT = Path(__file__).resolve().parent.parent

# The interpreter that has prefect_flows/'s own dependencies installed —
# NOT sys.executable (this backend's own venv doesn't have prefect/spacy/
# transformers/etc. installed, by design). Override via env var if the
# pipeline venv lives somewhere non-default. Cross-platform default: venvs
# put the interpreter at .venv/bin/python on Linux/macOS and
# .venv/Scripts/python.exe on Windows.
_default_venv_python = (
    REPO_ROOT / ".venv" / "Scripts" / "python.exe" if sys.platform == "win32"
    else REPO_ROOT / ".venv" / "bin" / "python"
)
PIPELINE_PYTHON = os.environ.get("PIPELINE_PYTHON", str(_default_venv_python))


def trigger_single_item_ingestion(
    location: str,
    is_local: bool,
    content_type: str,
    is_youtube: bool = False,
    raw_metadata: Optional[Dict] = None,
) -> subprocess.Popen:
    """Fire-and-forget: starts bronze_ingest.py in a separate process and
    returns immediately (does not wait for it to finish). Routes youtube
    URLs through --source youtube (required — a plain download won't work
    on a YouTube watch page), everything else through --source single."""
    if is_youtube and not is_local:
        cmd = [
            PIPELINE_PYTHON, "-m", "prefect_flows.bronze_ingest",
            "--source", "youtube",
            "--urls", location,
            "--audio-only" if content_type == "audio" else "",
        ]
    else:
        cmd = [
            PIPELINE_PYTHON, "-m", "prefect_flows.bronze_ingest",
            "--source", "single",
            "--location", location,
            "--type", content_type,
        ]
        if is_local:
            cmd.append("--is-local")
        if raw_metadata:
            cmd += ["--raw-metadata", json.dumps(raw_metadata)]

    print(
        f"[backend] Starting Bronze ingestion: {' '.join(cmd)}",
        flush=True,
    )

    return subprocess.Popen(
        cmd,
        cwd=str(REPO_ROOT),
    )