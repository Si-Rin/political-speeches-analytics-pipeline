"""
Thin wrapper around the backend API 

Keeps `requests` calls and error handling out of the page files.
"""
import os
from typing import Dict, List, Optional

import requests
import streamlit as st

API_BASE_URL = os.environ.get("API_BASE_URL", "http://localhost:8000")


def _get(path: str, **params) -> Optional[Dict]:
    try:
        resp = requests.get(f"{API_BASE_URL}{path}", params=params, timeout=15)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        st.error(f"Could not reach the backend at {API_BASE_URL}: {e}")
        return None


def _post(path: str, json_body: Dict) -> Optional[Dict]:
    try:
        resp = requests.post(f"{API_BASE_URL}{path}", json=json_body, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.HTTPError as e:
        detail = e.response.json().get("detail", str(e)) if e.response is not None else str(e)
        st.error(f"Request failed: {detail}")
        return None
    except requests.RequestException as e:
        st.error(f"Could not reach the backend at {API_BASE_URL}: {e}")
        return None


def probe_source(location: str, is_local: bool, content_type: str) -> Optional[Dict]:
    return _post("/documents/probe", {
        "location": location, "is_local": is_local, "content_type": content_type,
    })


def submit_source(location: str, is_local: bool, content_type: str, raw_metadata: Optional[Dict]) -> Optional[Dict]:
    return _post("/documents/submit", {
        "location": location, "is_local": is_local, "content_type": content_type,
        "raw_metadata": raw_metadata,
    })


def get_history(source_type: Optional[str] = None) -> List[Dict]:
    params = {}
    if source_type and source_type != "All":
        params["source_type"] = source_type
    result = _get("/documents/history", **params)
    return result["documents"] if result else []


def get_all_statuses(limit: int = 100) -> List[Dict]:
    result = _get("/status", limit=limit)
    return result["statuses"] if result else []


def get_status(doc_id: int) -> Optional[Dict]:
    return _get(f"/status/{doc_id}")
