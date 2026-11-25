"""
Run reconciliation on read. The flow finalizes its own run row, but a crashed worker, a killed process or a failed
Prefect submission leaves a row stuck in queued/running. `decide` compares the row with the Prefect flow run's state
and returns the correction (or None). Pure: the caller does the I/O.
"""
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

from backend.schemas.common import RunStatus, TERMINAL_STATUSES

_FAILED_STATES = {"FAILED", "CRASHED"}
_CANCELLED_STATES = {"CANCELLED", "CANCELLING"}
QUEUE_GRACE = timedelta(minutes=10)     # a row queued this long without a Prefect id lost its submission


def decide(status: RunStatus, prefect_flow_run_id: Optional[str], prefect_state_type: Optional[str],
           requested_at: datetime, now: Optional[datetime] = None) -> Optional[Tuple[RunStatus, str]]:
    """Return (new_status, error_message) when the row is inconsistent with reality, else None."""
    if status in TERMINAL_STATUSES:
        return None
    now = now or datetime.now(timezone.utc)
    if not prefect_flow_run_id:
        if now - requested_at > QUEUE_GRACE:
            return RunStatus.FAILED, "The run was never handed to Prefect (submission was lost)."
        return None
    if prefect_state_type is None:      # Prefect unreachable or run unknown: change nothing
        return None
    state = prefect_state_type.upper()
    if state in _FAILED_STATES:
        return RunStatus.FAILED, f"Prefect flow run ended as {state} before the run could record its result."
    if state in _CANCELLED_STATES:
        return RunStatus.CANCELLED, "Cancelled in Prefect."
    if state == "COMPLETED":
        return RunStatus.FAILED, "The flow finished but the run status was never recorded."
    return None
