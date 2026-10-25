"""Probe and History models (the Collection submit/crawl models are gone: runs replace them, see runs.py)."""
from datetime import date, datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from backend.schemas.common import ContentType


# ---------------------------------------------------------------------------
# Probe (review detected information before collecting)
# ---------------------------------------------------------------------------
class ProbeRequest(BaseModel):
    location: str = Field(..., description="A URL, or a local filesystem path")
    is_local: bool = Field(..., description="True if `location` is a local path, False if it's a URL")
    content_type: ContentType


class ProbeResponse(BaseModel):
    ok: bool
    detected_title: Optional[str] = None
    detected_duration_seconds: Optional[float] = None
    detected_uploader: Optional[str] = None
    raw_metadata: Optional[Dict] = None
    warnings: List[str] = []


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
