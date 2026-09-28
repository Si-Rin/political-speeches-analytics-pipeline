"""Run models: create, list, detail (with the discovery-log summary) and the discovery log itself."""
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from backend.schemas.common import CandidateStatus, MatchScope, Mode, ReasonCode, RunStatus


class RunCreated(BaseModel):
    run_id: str
    status: RunStatus
    prefect_flow_run_id: Optional[str] = None


class RunListItem(BaseModel):
    run_id: str
    mode: Mode
    source_name: str
    dry_run: bool
    status: RunStatus
    requested_at: datetime
    finished_at: Optional[datetime] = None
    documents_ingested: int = 0


class RunListResponse(BaseModel):
    runs: List[RunListItem]


class RunDetail(BaseModel):
    run_id: str
    prefect_flow_run_id: Optional[str] = None
    mode: Mode
    source_name: str
    params: Dict[str, Any]
    params_version: int
    dry_run: bool
    status: RunStatus
    listing_pages_visited: int
    error_message: Optional[str] = None
    requested_at: datetime
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    counts: Dict[str, int]              # discovery-log rows by CandidateStatus
    reasons: Dict[str, int]             # discovery-log rows by ReasonCode
    documents_ingested: int


class LogRow(BaseModel):
    id: int
    source_url: str
    canonical_url: str
    listing_url: Optional[str] = None
    title_hint: Optional[str] = None
    status: CandidateStatus
    reason_code: Optional[ReasonCode] = None
    detail: Optional[str] = None
    matched_keywords: Optional[List[str]] = None
    match_scope: Optional[MatchScope] = None
    doc_id: Optional[int] = None
    decided_at: datetime


class RunLogResponse(BaseModel):
    total: int
    items: List[LogRow]


class UploadResponse(BaseModel):
    staging_key: str
    file_name: str
    size_bytes: int
    content_type: Optional[str] = None
