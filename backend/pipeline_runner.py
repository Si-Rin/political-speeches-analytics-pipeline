"""
Submit Bronze ingestion runs to Prefect Server.

The FastAPI backend intentionally does not import Prefect or the flow module.
Instead, it calls the Prefect Server HTTP API and lets the configured Prefect
worker execute the deployment.

This keeps the backend environment lightweight and preserves the dependency
separation between the API and the Prefect pipeline.
"""
from typing import Any, Dict, Optional
from urllib.parse import quote

import os
import requests


PREFECT_API_URL = os.environ.get("PREFECT_API_URL", "http://localhost:4200/api").rstrip("/")
PREFECT_FLOW_NAME = os.environ.get("PREFECT_FLOW_NAME", "bronze-ingestion")
PREFECT_DEPLOYMENT_NAME = os.environ.get("PREFECT_DEPLOYMENT_NAME", "default")
PREFECT_DEPLOYMENT_ID = os.environ.get("PREFECT_DEPLOYMENT_ID")
PREFECT_API_TIMEOUT = float(os.environ.get("PREFECT_API_TIMEOUT", "10"))


def _prefect_request(method: str, path: str, **kwargs) -> requests.Response:
    """Call the Prefect Server API with a consistent timeout and error handling."""
    url = f"{PREFECT_API_URL}{path}"
    try:
        response = requests.request(
            method,
            url,
            timeout=PREFECT_API_TIMEOUT,
            **kwargs,
        )
    except requests.RequestException as exc:
        raise RuntimeError(
            f"Could not reach Prefect Server at {PREFECT_API_URL}: {exc}"
        ) from exc

    if not response.ok:
        detail = response.text.strip()
        raise RuntimeError(
            f"Prefect Server returned HTTP {response.status_code} for {path}"
            + (f": {detail}" if detail else "")
        )

    return response


def _get_deployment_id() -> str:
    """Resolve the deployment ID from configuration or its flow/deployment name."""
    if PREFECT_DEPLOYMENT_ID:
        return PREFECT_DEPLOYMENT_ID

    flow_name = quote(PREFECT_FLOW_NAME, safe="")
    deployment_name = quote(PREFECT_DEPLOYMENT_NAME, safe="")
    response = _prefect_request(
        "GET",
        f"/deployments/name/{flow_name}/{deployment_name}",
    )
    deployment = response.json()

    deployment_id = deployment.get("id")
    if not deployment_id:
        raise RuntimeError(
            f"Prefect deployment '{PREFECT_FLOW_NAME}/{PREFECT_DEPLOYMENT_NAME}' "
            "was found but has no deployment ID."
        )

    return deployment_id


def _submit_bronze_flow(parameters: Dict[str, Any]) -> Dict[str, Any]:
    """Create and return a Prefect flow run for the Bronze deployment."""
    deployment_id = _get_deployment_id()

    response = _prefect_request(
        "POST",
        f"/deployments/{deployment_id}/create_flow_run",
        json={"parameters": parameters},
        headers={"Content-Type": "application/json"},
    )

    return response.json()


def trigger_single_item_ingestion(
    location: str,
    is_local: bool,
    content_type: str,
    source_kind: str = "single",
    raw_metadata: Optional[Dict] = None,
) -> Dict[str, Any]:
    """Submit one Bronze ingestion run to Prefect.

    ``source_kind`` is produced by ``classify_source()`` and maps directly to
    the corresponding source adapter in ``prefect_flows/bronze_ingest.py``.
    """
    if source_kind == "youtube":
        parameters = {
            "source_name": "youtube",
            "urls": [location],
            "audio_only": content_type == "audio",
            "playlist_mode": False,
            "max_downloads": None,
        }
    elif source_kind == "miller_center":
        parameters = {
            "source_name": "miller_center",
            "urls": [location],
            "start_url": None,
            "max_depth": 100,
        }
    elif source_kind == "internet_archive":
        parameters = {
            "source_name": "internet_archive",
            "urls": [location],
            "excluded_indices": {},
        }
    else:
        parameters = {
            "source_name": "single",
            "location": location,
            "content_type": content_type,
            "is_local": is_local,
            "raw_metadata": raw_metadata,
        }

    run = _submit_bronze_flow(parameters)
    print(
        f"[backend] Submitted Bronze flow run: flow_run_id={run.get('id')}",
        flush=True,
    )
    return run


def trigger_crawl(
    seed_urls: list,
    keywords: list,
    allowed_domains: Optional[list] = None,
    max_depth: Optional[int] = None,
    max_pages: Optional[int] = None,
) -> Dict[str, Any]:
    """Submit a keyword-based web crawl to the Bronze Prefect deployment."""
    parameters = {
        "source_name": "web_crawl",
        "seed_urls": seed_urls,
        "keywords": keywords,
        "allowed_domains": allowed_domains,
        "max_depth": 2 if max_depth is None else max_depth,
        "max_pages": 50 if max_pages is None else max_pages,
    }

    run = _submit_bronze_flow(parameters)
    print(
        f"[backend] Submitted Bronze crawl run: flow_run_id={run.get('id')}",
        flush=True,
    )
    return run
