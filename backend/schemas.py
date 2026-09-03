"""
Pydantic request/response models for the Collection API.

No business logic here — validation and shape only
See pipeline_probe.py (metadata detection) and pipeline_runner.py (triggering bronze_ingest.py) for the actual logic these shapes wrap.
"""
from datetime import date, datetime
from typing import Dict, List, Literal, Optional

from pydantic import BaseModel, Field

ContentType = Literal["text", "video", "audio"]


# ---------------------------------------------------------------------------
# Collection: probe (review detected information before submitting)
# ---------------------------------------------------------------------------
class ProbeRequest(BaseModel):
    location: str = Field(..., description="A URL, or a local filesystem path")
    is_local: bool = Field(..., description="True if `location` is a local path, False if it's a URL")
    content_type: ContentType


class ProbeResponse(BaseModel):
    ok: bool
    detected_title: Optional[str] = None
    detected_file_name: Optional[str] = None
    detected_mime_type: Optional[str] = None
    detected_size_bytes: Optional[int] = None
    detected_duration_seconds: Optional[float] = None
    detected_uploader: Optional[str] = None
    raw_metadata: Optional[Dict] = None
    warnings: List[str] = []


# ---------------------------------------------------------------------------
# Collection: submit
# ---------------------------------------------------------------------------
class SubmitRequest(BaseModel):
    location: str
    is_local: bool
    content_type: ContentType
    raw_metadata: Optional[Dict] = None  # normally echoed back from ProbeResponse
    title: Optional[str] = None
    publication_date: Optional[date] = None
    language: Optional[str] = None
    notes: Optional[str] = None


class SubmitResponse(BaseModel):
    accepted: bool
    message: str
    # No doc_id, bronze_ingest.py runs out-of-process (subprocess) and only assigns a doc_id once the download/checksum/insert completes — see pipeline_runner.py.
    # Poll GET /documents (filtered by `location`) or GET /documents/history to see it land.
    location: str


# ---------------------------------------------------------------------------
# Collection: crawl (discovers multiple documents, not just one)
# ---------------------------------------------------------------------------
class CrawlRequest(BaseModel):
    seed_urls: List[str] = Field(..., min_length=1)
    keywords: List[str] = Field(..., min_length=1)
    allowed_domains: Optional[List[str]] = None
    max_depth: Optional[int] = Field(default=None, ge=2, le=5)
    max_pages: Optional[int] = Field(default=None, ge=1, le=500)


class CrawlResponse(BaseModel):
    accepted: bool
    message: str


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------
class DocumentSummary(BaseModel):
    doc_id: int
    source: str  # source_url, or file_name for local files
    source_type: ContentType
    speaker: Optional[str] = None
    title: Optional[str] = None
    publication_date: Optional[date] = None
    ingestion_date: datetime
    silver_status: str  # 'not_started' | 'pending' | 'success' | 'failed'


class DocumentHistoryResponse(BaseModel):
    documents: List[DocumentSummary]


# ---------------------------------------------------------------------------
# Pipeline status
# ---------------------------------------------------------------------------
class GoldModuleStatus(BaseModel):
    module: str
    done: bool


class PipelineStatus(BaseModel):
    doc_id: int
    bronze_status: str  # 'success' (bronze insert is atomic — if the row exists, it succeeded)
    silver_status: str  # 'not_started' | 'pending' | 'success' | 'failed'
    silver_error: Optional[str] = None
    gold_modules: List[GoldModuleStatus]
    gold_status: str  # 'not_started' | 'partial' | 'success'


class PipelineStatusResponse(BaseModel):
    statuses: List[PipelineStatus]