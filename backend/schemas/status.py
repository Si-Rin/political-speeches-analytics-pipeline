"""Pipeline status models."""
from typing import Any, List, Optional

from pydantic import BaseModel


# ---------------------------------------------------------------------------
# Pipeline status
# ---------------------------------------------------------------------------
class GoldModuleStatus(BaseModel):
    module: str
    done: bool
    result: Optional[Any] = None


class PipelineStatus(BaseModel):
    doc_id: int
    bronze_status: str  # 'success' (bronze insert is atomic — if the row exists, it succeeded)
    silver_status: str  # 'not_started' | 'pending' | 'success' | 'failed'
    silver_error: Optional[str] = None
    gold_modules: List[GoldModuleStatus]
    gold_status: str  # 'not_started' | 'partial' | 'success'


class PipelineStatusResponse(BaseModel):
    statuses: List[PipelineStatus]
