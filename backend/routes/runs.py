"""Collection runs: create (single or archive, optionally a dry-run preview), list, detail, discovery log, cancel."""
import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Query

from backend import pipeline_runner, reconcile, runs_repo, storage
from backend.prefect_params import to_prefect_params
from backend.schemas import (
    PARAMS_VERSION, LocalFileRequest, RunCreated, RunDetail, RunListItem, RunListResponse, RunLogResponse, RunRequest,
    RunStatus, TERMINAL_STATUSES, CandidateStatus, ReasonCode,
)

router = APIRouter()


def _reconciled(run: dict) -> dict:
    """Correct a run stuck in queued/running when Prefect says the flow run is over (or was never submitted)."""
    status = RunStatus(run["status"])
    if status in TERMINAL_STATUSES:
        return run
    state = pipeline_runner.get_flow_run_state(run["prefect_flow_run_id"]) if run["prefect_flow_run_id"] else None
    decision = reconcile.decide(status, run["prefect_flow_run_id"], state, run["requested_at"])
    if decision and runs_repo.mark_terminal(run["run_id"], decision[0], decision[1]):
        return runs_repo.get_run(run["run_id"]) or run
    return run


@router.post("/runs", response_model=RunCreated, status_code=202)
def create_run(req: RunRequest):
    """Create a run and hand it to Prefect; returns immediately. `dry_run: true` records what would be collected."""
    if isinstance(req, LocalFileRequest):
        try:
            found = storage.staging_exists(req.staging_key)
        except storage.StorageUnavailable as exc:
            raise HTTPException(status_code=503, detail=str(exc))
        if not found:
            raise HTTPException(status_code=422, detail="Unknown staging_key: upload the file again (staged files expire).")

    payload = to_prefect_params(req)
    run_id = str(uuid.uuid4())
    runs_repo.create_run(run_id, payload.mode.value, payload.source_name.value, payload.params, PARAMS_VERSION, req.dry_run)
    try:
        prefect_id = pipeline_runner.create_flow_run(run_id, payload.source_name.value, payload.params, PARAMS_VERSION, req.dry_run)
    except pipeline_runner.PrefectError as exc:
        runs_repo.mark_terminal(run_id, RunStatus.FAILED, str(exc))
        raise HTTPException(status_code=502, detail={"run_id": run_id, "message": str(exc)})
    runs_repo.set_prefect_id(run_id, prefect_id)
    return RunCreated(run_id=run_id, status=RunStatus.QUEUED, prefect_flow_run_id=prefect_id)


@router.get("/runs", response_model=RunListResponse)
def list_runs(limit: int = Query(50, ge=1, le=200), status: Optional[RunStatus] = None):
    rows = runs_repo.list_runs(limit, status.value if status else None)
    return RunListResponse(runs=[RunListItem(**r) for r in rows])


@router.get("/runs/{run_id}", response_model=RunDetail)
def get_run(run_id: uuid.UUID):
    run = runs_repo.get_run(str(run_id))
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    run = _reconciled(run)
    counts, reasons = runs_repo.summary(run["run_id"])
    return RunDetail(**run, counts=counts, reasons=reasons, documents_ingested=counts.get("ingested", 0))


@router.get("/runs/{run_id}/log", response_model=RunLogResponse)
def get_run_log(run_id: uuid.UUID, status: Optional[CandidateStatus] = None, reason_code: Optional[ReasonCode] = None,
                limit: int = Query(200, ge=1, le=1000), offset: int = Query(0, ge=0)):
    if runs_repo.get_run(str(run_id)) is None:
        raise HTTPException(status_code=404, detail="Run not found")
    total, rows = runs_repo.log_rows(str(run_id), status.value if status else None,
                                     reason_code.value if reason_code else None, limit, offset)
    return RunLogResponse(total=total, items=rows)


@router.post("/runs/{run_id}/cancel", response_model=RunDetail)
def cancel_run(run_id: uuid.UUID):
    run = runs_repo.get_run(str(run_id))
    if run is None:
        raise HTTPException(status_code=404, detail="Run not found")
    if RunStatus(run["status"]) in TERMINAL_STATUSES:
        raise HTTPException(status_code=409, detail=f"Run already {run['status']}")
    if run["prefect_flow_run_id"]:
        try:
            pipeline_runner.cancel_flow_run(run["prefect_flow_run_id"])
        except pipeline_runner.PrefectError as exc:
            raise HTTPException(status_code=502, detail=str(exc))
    runs_repo.mark_terminal(run["run_id"], RunStatus.CANCELLED, "Cancelled by the user.")
    return get_run(run_id)
