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



# ---------------------------------------------------------------------------
# Documents page: library, detail, text preview, metadata edit, delete
# ---------------------------------------------------------------------------
class DocumentRecord(BaseModel):
    doc_id: int
    source: str                                   # source_url (file_name for local files)
    source_type: ContentType
    file_name: Optional[str] = None
    file_size: Optional[int] = None
    mime_type: Optional[str] = None
    ingestion_date: datetime
    origin: Optional[str] = None                  # collection source (youtube, miller_center, ...)
    title: Optional[str] = None
    speaker: Optional[str] = None
    publication_date: Optional[date] = None
    language: Optional[str] = None
    word_count: Optional[int] = None
    silver_status: str = "not_started"
    silver_error: Optional[str] = None
    gold_done: int = 0
    gold_total: int = 0


class DocumentLibraryResponse(BaseModel):
    documents: List[DocumentRecord]


class DocumentDetail(DocumentRecord):
    checksum: str
    storage_path: str
    first_run_id: Optional[str] = None
    char_count: Optional[int] = None
    processing_date: Optional[datetime] = None
    raw_metadata: Optional[Dict] = None


class TextPreview(BaseModel):
    text: str
    offset: int
    total_chars: int
    has_more: bool


class MetadataUpdate(BaseModel):
    """Only the fields present in the request are changed; sending null clears a field."""
    title: Optional[str] = Field(default=None, max_length=500)
    speaker: Optional[str] = Field(default=None, max_length=200)
    publication_date: Optional[date] = None
    language: Optional[str] = Field(default=None, max_length=20)


class DeleteRequest(BaseModel):
    doc_ids: List[int] = Field(..., min_length=1, max_length=500)


class DeleteResponse(BaseModel):
    deleted: int
    storage_errors: int = 0
