"""Trigger and follow the corpus-level topic modeling (BERTopic) from the UI, no command line needed."""
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from backend import pipeline_runner
from backend.db import get_connection

router = APIRouter()

# Must match prefect_flows.gold_topics / analytics.topics (the backend does not import the flows; a test asserts equality)
SHORT_FORM_WORD_COUNT_THRESHOLD = 500
MIN_DOCS_FOR_TOPIC_MODELING = 20

_STATE_MAP = {
    "SCHEDULED": "queued", "PENDING": "queued", "RUNNING": "running", "COMPLETED": "completed",
    "FAILED": "failed", "CRASHED": "failed", "CANCELLING": "cancelled", "CANCELLED": "cancelled",
}
_ACTIVE = ("queued", "running")


class TopicsRunStatus(BaseModel):
    long_form_docs: int
    short_form_docs: int
    min_docs: int
    can_run: bool                     # at least one corpus has enough documents
    state: str                        # idle | queued | running | completed | failed | cancelled
    message: Optional[str] = None     # failure message from Prefect, if any
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


def map_state(state_type: Optional[str]) -> str:
    return _STATE_MAP.get((state_type or "").upper(), "idle")


def _eligible_counts() -> tuple:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT count(*) FILTER (WHERE COALESCE(s.word_count, 0) >= %s),
                          count(*) FILTER (WHERE COALESCE(s.word_count, 0) <  %s)
                   FROM silver.text s JOIN bronze.documents b ON b.doc_id = s.doc_id
                   WHERE s.status_processing = 'success' AND s.transcript IS NOT NULL AND b.excluded = FALSE""",
                (SHORT_FORM_WORD_COUNT_THRESHOLD, SHORT_FORM_WORD_COUNT_THRESHOLD))
            long_n, short_n = cur.fetchone()
    finally:
        conn.close()
    return int(long_n), int(short_n)


def build_status(long_n: int, short_n: int, last_run: Optional[dict]) -> TopicsRunStatus:
    state = map_state((last_run or {}).get("state_type"))
    return TopicsRunStatus(
        long_form_docs=long_n, short_form_docs=short_n, min_docs=MIN_DOCS_FOR_TOPIC_MODELING,
        can_run=max(long_n, short_n) >= MIN_DOCS_FOR_TOPIC_MODELING, state=state,
        message=(last_run or {}).get("state_message") if state == "failed" else None,
        started_at=(last_run or {}).get("start_time"), finished_at=(last_run or {}).get("end_time"))


@router.get("/analytics/topics/run", response_model=TopicsRunStatus)
def get_topics_run_status():
    long_n, short_n = _eligible_counts()
    return build_status(long_n, short_n, pipeline_runner.latest_topics_flow_run())


@router.post("/analytics/topics/run", response_model=TopicsRunStatus, status_code=202)
def start_topics_run():
    long_n, short_n = _eligible_counts()
    last = pipeline_runner.latest_topics_flow_run()
    status = build_status(long_n, short_n, last)
    if not status.can_run:
        raise HTTPException(status_code=422, detail=(
            f"Not enough documents for topic modeling: need at least {MIN_DOCS_FOR_TOPIC_MODELING} processed documents in one group "
            f"(long speeches: {long_n}, short posts: {short_n})."))
    if status.state in _ACTIVE:
        raise HTTPException(status_code=409, detail="A topic modeling run is already in progress.")
    try:
        pipeline_runner.create_topics_flow_run()
    except pipeline_runner.PrefectError as exc:
        raise HTTPException(status_code=502, detail=str(exc))
    return build_status(long_n, short_n, {"state_type": "SCHEDULED"})
