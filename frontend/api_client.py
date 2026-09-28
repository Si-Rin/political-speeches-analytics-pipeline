"""
Thin wrapper around the backend API.

Keeps `requests` calls and error handling out of the page files. Every function that hits the backend returns
None on failure (after showing st.error), so pages just check truthiness.
"""
import os
from datetime import date
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")


def _url(path: str) -> str:
    return f"{API_BASE_URL}{path}"


def _error_detail(resp: requests.Response) -> str:
    try:
        detail = resp.json().get("detail", resp.text)
    except ValueError:
        detail = resp.text
    return detail if isinstance(detail, str) else str(detail)


def _get(path: str, **params) -> Optional[Dict]:
    try:
        resp = requests.get(_url(path), params={k: v for k, v in params.items() if v is not None}, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        st.error(f"Request failed: {_error_detail(e.response)}")
        return None
    except requests.RequestException as e:
        st.error(f"Could not reach the backend at {API_BASE_URL}: {e}")
        return None


def _post(path: str, json_body: Optional[Dict] = None) -> Optional[Dict]:
    try:
        resp = requests.post(_url(path), json=json_body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        st.error(f"Request failed: {_error_detail(e.response)}")
        return None
    except requests.RequestException as e:
        st.error(f"Could not reach the backend at {API_BASE_URL}: {e}")
        return None


# ---------------------------------------------------------------------------
# Probe (single-mode "review detected information" step)
# ---------------------------------------------------------------------------
def probe_source(location: str, is_local: bool, content_type: str) -> Optional[Dict]:
    return _post("/documents/probe", {"location": location, "is_local": is_local, "content_type": content_type})


# ---------------------------------------------------------------------------
# Uploads
# ---------------------------------------------------------------------------
def upload_file(file_name: str, data: bytes, content_type: Optional[str]) -> Optional[Dict]:
    try:
        resp = requests.post(_url("/uploads"), files={"file": (file_name, data, content_type or "application/octet-stream")}, timeout=120)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        st.error(f"Upload failed: {_error_detail(e.response)}")
        return None
    except requests.RequestException as e:
        st.error(f"Could not reach the backend at {API_BASE_URL}: {e}")
        return None


# ---------------------------------------------------------------------------
# Runs
# ---------------------------------------------------------------------------
def create_run(source: str, dry_run: bool = False, **fields: Any) -> Optional[Dict]:
    """
    `fields` are the source-specific request fields (see backend/schemas/sources.py); dates are stringified.
    POST /runs can fail two ways: validation (422, nothing created) or a Prefect outage (502, the run row exists
    but is marked failed) -- the caller needs to tell those apart, so this bypasses the generic _post helper.
    """
    body = {"source": source, "dry_run": dry_run}
    for k, v in fields.items():
        if v is None:
            continue
        body[k] = v.isoformat() if isinstance(v, date) else v
    try:
        resp = requests.post(_url("/runs"), json=body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        if e.response is not None and e.response.status_code == 502:
            detail = e.response.json().get("detail", {})
            st.error(f"The run was created but could not be handed to Prefect: {detail.get('message', detail)}")
            return {"run_id": detail.get("run_id"), "status": "failed"} if isinstance(detail, dict) else None
        st.error(f"Request failed: {_error_detail(e.response)}")
        return None
    except requests.RequestException as e:
        st.error(f"Could not reach the backend at {API_BASE_URL}: {e}")
        return None


def list_runs(limit: int = 50, status: Optional[str] = None) -> List[Dict]:
    result = _get("/runs", limit=limit, status=status)
    return result["runs"] if result else []


def get_run(run_id: str) -> Optional[Dict]:
    return _get(f"/runs/{run_id}")


def get_run_log(run_id: str, status: Optional[str] = None, reason_code: Optional[str] = None,
                limit: int = 200, offset: int = 0) -> Optional[Dict]:
    return _get(f"/runs/{run_id}/log", status=status, reason_code=reason_code, limit=limit, offset=offset)


def cancel_run(run_id: str) -> Optional[Dict]:
    return _post(f"/runs/{run_id}/cancel")


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------
def get_history(source_type: Optional[str] = None) -> List[Dict]:
    result = _get("/documents/history", source_type=source_type if source_type != "All" else None)
    return result["documents"] if result else []


def get_all_statuses() -> List[Dict]:
    result = _get("/status")
    return result["statuses"] if result else []


def get_status(doc_id: int) -> Optional[Dict]:
    return _get(f"/status/{doc_id}")


def get_overview() -> Optional[Dict]:
    return _get("/analytics/overview")


def get_topics() -> Optional[Dict]:
    return _get("/analytics/topics")


def get_labels() -> Optional[Dict]:
    return _get("/analytics/labels")


def get_sentiment() -> Optional[Dict]:
    return _get("/analytics/sentiment")


def get_emotions() -> Optional[Dict]:
    return _get("/analytics/emotions")


def get_entities(top_n: int = 15) -> Optional[Dict]:
    return _get("/analytics/entities", top_n=top_n)
