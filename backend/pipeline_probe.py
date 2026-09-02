"""
Lightweight metadata detection for the Collection UI's "review detected information" step.
No download to MinIO, no DB writes, no dedup check — this only looks, it doesn't ingest
Actual ingestion happens in pipeline_runner.py after the user confirms.

Kept deliberately separate from routes/documents.py so the HTTP layer stays thin.
"""
import mimetypes
import os
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

import requests

YOUTUBE_DOMAINS = {"youtube.com", "www.youtube.com", "youtu.be", "m.youtube.com"}
MILLER_CENTER_DOMAINS = {"millercenter.org", "www.millercenter.org"}
INTERNET_ARCHIVE_DOMAINS = {"archive.org", "www.archive.org"}


def is_youtube_url(location: str) -> bool:
    try:
        return urlparse(location).netloc.lower() in YOUTUBE_DOMAINS
    except ValueError:
        return False
    
    
def classify_source(location: str, is_local: bool) -> str:
    """
    Single source of truth for URL -> adapter routing.
    Used by both probe() (to preview correctly) and pipeline_runner.py (to trigger the right bronze_ingest.py --source) 
    Kept in one place so the two never drift out of sync with each other."""
    if is_local:
        return "single"
    try:
        domain = urlparse(location).netloc.lower()
    except ValueError:
        return "single"
 
    if domain in YOUTUBE_DOMAINS:
        return "youtube"
    if domain in MILLER_CENTER_DOMAINS:
        return "miller_center"
    if domain in INTERNET_ARCHIVE_DOMAINS:
        return "internet_archive"
    return "single"


def probe(location: str, is_local: bool, content_type: str) -> Dict:
    """Returns a dict matching schemas.ProbeResponse's fields (as a plain
    dict, not the model itself, so this module has no FastAPI/pydantic
    dependency)."""
    if is_local:
        return _probe_local(location)
    if is_youtube_url(location):
        return _probe_youtube(location)
    if content_type == "text":
        return _probe_text_url(location)
    return _probe_direct_url(location)


def _probe_local(location: str) -> Dict:
    path = Path(location)
    if not path.exists() or not path.is_file():
        return {"ok": False, "warnings": [f"File not found: {location}"]}

    mime_type, _ = mimetypes.guess_type(str(path))
    return {
        "ok": True,
        "detected_file_name": path.name,
        "detected_mime_type": mime_type,
        "detected_size_bytes": os.path.getsize(path),
        "warnings": [],
    }


def _probe_youtube(location: str) -> Dict:
    try:
        import yt_dlp
    except ImportError:
        return {"ok": False, "warnings": ["yt-dlp is not installed on the backend"]}

    try:
        with yt_dlp.YoutubeDL({"quiet": True, "skip_download": True}) as ydl:
            info = ydl.extract_info(location, download=False)
    except Exception as e:
        return {"ok": False, "warnings": [f"Could not read YouTube metadata: {e}"]}

    return {
        "ok": True,
        "detected_title": info.get("title"),
        "detected_duration_seconds": info.get("duration"),
        "detected_uploader": info.get("uploader") or info.get("channel"),
        "raw_metadata": {
            "uploader": info.get("uploader"),
            "channel": info.get("channel"),
            "upload_date": info.get("upload_date"),  # YYYYMMDD string
            "webpage_url": info.get("webpage_url"),
        },
        "warnings": [],
    }


def _probe_text_url(location: str) -> Dict:
    try:
        resp = requests.get(location, timeout=10, stream=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"ok": False, "warnings": [f"Could not fetch URL: {e}"]}

    warnings = []
    title = None
    try:
        head = next(resp.iter_content(chunk_size=8192, decode_unicode=False))
        text_head = head.decode(resp.encoding or "utf-8", errors="ignore")
        if "<title" in text_head.lower():
            start = text_head.lower().find("<title")
            start = text_head.find(">", start) + 1
            end = text_head.lower().find("</title>", start)
            if start > 0 and end > start:
                title = text_head[start:end].strip()
    except (StopIteration, UnicodeDecodeError):
        warnings.append("Could not preview page content for a title")
    finally:
        resp.close()

    content_length = resp.headers.get("Content-Length")
    return {
        "ok": True,
        "detected_title": title,
        "detected_mime_type": resp.headers.get("Content-Type", "").split(";")[0] or None,
        "detected_size_bytes": int(content_length) if content_length else None,
        "warnings": warnings,
    }


def _probe_direct_url(location: str) -> Dict:
    """For direct media file links (video/audio) that aren't YouTube —
    a HEAD request is enough to preview size/type without downloading."""
    try:
        resp = requests.head(location, timeout=10, allow_redirects=True)
        resp.raise_for_status()
    except requests.RequestException as e:
        return {"ok": False, "warnings": [f"Could not reach URL: {e}"]}

    content_length = resp.headers.get("Content-Length")
    file_name = Path(urlparse(location).path).name or None
    return {
        "ok": True,
        "detected_file_name": file_name,
        "detected_mime_type": resp.headers.get("Content-Type", "").split(";")[0] or None,
        "detected_size_bytes": int(content_length) if content_length else None,
        "warnings": [],
    }