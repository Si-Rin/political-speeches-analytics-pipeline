"""
Thin Prefect client. The backend does not import Prefect or prefect_flows: it calls the Prefect Server HTTP API and
the configured worker executes the deployment. The only contract is the flow's parameters
(run_id, source_name, params, params_version, dry_run) and the run row in collection.runs.
"""
import os
from typing import Any, Dict, Optional
from urllib.parse import quote

import requests

PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://localhost:4200/api").rstrip("/")
PREFECT_FLOW_NAME = os.environ.get("PREFECT_FLOW_NAME", "bronze-ingestion")
PREFECT_DEPLOYMENT_NAME = os.environ.get("PREFECT_DEPLOYMENT_NAME", "default")
PREFECT_DEPLOYMENT_ID = os.environ.get("PREFECT_DEPLOYMENT_ID")
PREFECT_API_TIMEOUT = float(os.environ.get("PREFECT_API_TIMEOUT", "10"))


class PrefectError(RuntimeError):
    """Prefect Server unreachable, or it rejected the request."""


def _request(method: str, path: str, **kwargs) -> requests.Response:
    url = f"{PREFECT_API_URL}{path}"
    try:
        response = requests.request(method, url, timeout=PREFECT_API_TIMEOUT, **kwargs)
    except requests.RequestException as exc:
        raise PrefectError(f"Could not reach Prefect Server at {PREFECT_API_URL}: {exc}") from exc
    if not response.ok:
        detail = response.text.strip()
        raise PrefectError(f"Prefect Server returned HTTP {response.status_code} for {path}" + (f": {detail}" if detail else ""))
    return response


def _deployment_id() -> str:
    if PREFECT_DEPLOYMENT_ID:
        return PREFECT_DEPLOYMENT_ID
    flow = quote(PREFECT_FLOW_NAME, safe="")
    deployment = quote(PREFECT_DEPLOYMENT_NAME, safe="")
    found = _request("GET", f"/deployments/name/{flow}/{deployment}").json().get("id")
    if not found:
        raise PrefectError(f"Deployment '{PREFECT_FLOW_NAME}/{PREFECT_DEPLOYMENT_NAME}' has no id.")
    return found


def create_flow_run(run_id: str, source_name: str, params: Dict[str, Any], params_version: int, dry_run: bool) -> str:
    """Submit one run and return the Prefect flow run id."""
    body = {"parameters": {"run_id": run_id, "source_name": source_name, "params": params,
                           "params_version": params_version, "dry_run": dry_run}}
    response = _request("POST", f"/deployments/{_deployment_id()}/create_flow_run", json=body,
                        headers={"Content-Type": "application/json"})
    flow_run_id = response.json().get("id")
    if not flow_run_id:
        raise PrefectError("Prefect accepted the run but returned no flow run id.")
    return flow_run_id


def get_flow_run_state(flow_run_id: str) -> Optional[str]:
    """State type of a flow run (SCHEDULED, PENDING, RUNNING, COMPLETED, FAILED, CRASHED, CANCELLING, CANCELLED, ...).
    None when Prefect cannot be reached or does not know the run: callers must treat that as 'no information'."""
    try:
        state = _request("GET", f"/flow_runs/{quote(flow_run_id, safe='')}").json().get("state") or {}
    except PrefectError:
        return None
    return state.get("type")


def cancel_flow_run(flow_run_id: str) -> None:
    _request("POST", f"/flow_runs/{quote(flow_run_id, safe='')}/set_state",
             json={"state": {"type": "CANCELLING", "name": "Cancelling"}, "force": True})
