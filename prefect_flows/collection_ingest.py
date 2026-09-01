"""Single-document collection flow used by the Streamlit UI.

The UI should collect user-friendly metadata, while this flow reuses the
existing Bronze staging/upload tasks so the storage and deduplication logic
remains in one place.
"""

import mimetypes
from pathlib import Path
from typing import Optional
from urllib.parse import urlparse

from prefect import flow, get_run_logger

from prefect_flows.bronze_ingest import checksum_and_stage, is_duplicate, upload_and_register
from prefect_flows.sources.base import Candidate
from prefect_flows.sources.url_s import UrlListSource
from prefect_flows.sources.youtube import YoutubeSource


_ALLOWED_EXTENSIONS = {
    ".mp4": "video",
    ".mov": "video",
    ".mkv": "video",
    ".avi": "video",
    ".mp3": "audio",
    ".wav": "audio",
    ".m4a": "audio",
    ".flac": "audio",
    ".webm": "audio",
    ".txt": "text",
    ".html": "text",
    ".htm": "text",
    ".pdf": "text",
}


def _local_candidate(path: str, content_type: str, metadata: dict) -> Candidate:
    file_path = Path(path).resolve()
    if not file_path.exists():
        raise FileNotFoundError(f"Local file not found: {file_path}")
    if not file_path.is_file():
        raise ValueError(f"Local path is not a file: {file_path}")

    suffix = file_path.suffix.lower()
    detected_type = _ALLOWED_EXTENSIONS.get(suffix)
    if detected_type is None:
        raise ValueError(
            f"Unsupported file type '{suffix or 'unknown'}'. "
            f"Supported formats: {', '.join(sorted(_ALLOWED_EXTENSIONS))}"
        )

    mime_type, _ = mimetypes.guess_type(str(file_path))
    if content_type != detected_type:
        raise ValueError(
            f"The selected content type is '{content_type}', but the file extension "
            f"is normally treated as '{detected_type}'."
        )

    return Candidate(
        source_url=str(file_path),
        source_type=content_type,
        file_name=file_path.name,
        is_local=True,
        mime_type=mime_type,
        raw_metadata=metadata,
    )


def _url_candidate(url: str, content_type: str, metadata: dict) -> Candidate:
    source = UrlListSource(urls=[url], source_type=content_type)
    candidate = next(source.discover())
    candidate.raw_metadata = metadata
    return candidate


@flow(name="collect-document")
def collect_document(
    source_kind: str,
    content_type: str,
    metadata: Optional[dict] = None,
    url: Optional[str] = None,
    local_path: Optional[str] = None,
    youtube_audio_only: bool = False,
) -> dict:
    """Collect exactly one user-submitted document into Bronze.

    source_kind: "url", "youtube", or "local"
    content_type: "text", "audio", or "video"
    """
    logger = get_run_logger()
    metadata = metadata or {}

    if content_type not in {"text", "audio", "video"}:
        raise ValueError("content_type must be text, audio, or video")

    if source_kind == "local":
        if not local_path:
            raise ValueError("A local path is required")
        candidate = _local_candidate(local_path, content_type, metadata)

    elif source_kind == "youtube":
        if not url:
            raise ValueError("A YouTube URL is required")
        audio_only = content_type == "audio" or youtube_audio_only
        source = YoutubeSource(urls=[url], audio_only=audio_only, playlist_mode=False)
        candidates = list(source.discover())
        if not candidates:
            raise ValueError("The YouTube URL could not be downloaded")
        candidate = candidates[0]
        candidate.raw_metadata = {**(candidate.raw_metadata or {}), **metadata}

    elif source_kind == "url":
        if not url:
            raise ValueError("A URL is required")
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"}:
            raise ValueError("URL must start with http:// or https://")
        candidate = _url_candidate(url, content_type, metadata)

    else:
        raise ValueError(f"Unknown source kind: {source_kind}")

    staged = checksum_and_stage(candidate)
    checksum = staged["checksum"]

    if is_duplicate(checksum):
        staged_path = staged["tmp_path"]
        Path(staged_path).unlink(missing_ok=True)
        logger.info("Document skipped because the same content already exists")
        return {
            "status": "duplicate",
            "file_name": candidate.file_name,
            "checksum": checksum,
            "doc_id": None,
        }

    doc_id = upload_and_register(staged)
    logger.info(f"Document collected successfully: doc_id={doc_id}")
    return {
        "status": "ingested",
        "file_name": candidate.file_name,
        "checksum": checksum,
        "doc_id": doc_id,
    }
