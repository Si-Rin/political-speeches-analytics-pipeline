"""
Route: Pipeline Status (Bronze / Silver / Gold state per document).

gold.analytics has no single status column 
Each Gold flow (see gold_linguistic.py, gold_sentiment.py, gold_labels.py) independently populates its own JSONB column(s)
So "Gold status" for a document is derived here from which of those columns are non-NULL.
"""
from typing import List, Optional

from fastapi import APIRouter

from prefect_flows.clients import get_postgres_connection

from backend.schemas import PipelineStatus, PipelineStatusResponse, GoldModuleStatus

router = APIRouter()

# Only the modules that actually have a flow populating them today (see the Stage A / Stage B plan) 
# Topics/keywords have DB columns reserved but no flow wired yet, so they're deliberately excluded here rather than showing every document as permanently "pending" on those.
GOLD_MODULES = [
    "lex_metrics",
    "syntactic_metrics",
    "entities",
    "sentiment",
    "emotions",
    "labels",
]


def _gold_status_for_row(gold_row: Optional[tuple]) -> tuple[List[GoldModuleStatus], str]:
    if gold_row is None:
        return (
            [GoldModuleStatus(module=m, done=False) for m in GOLD_MODULES],
            "not_started",
        )

    modules = [
        GoldModuleStatus(module=m, done=val is not None)
        for m, val in zip(GOLD_MODULES, gold_row)
    ]
    done_count = sum(1 for m in modules if m.done)
    if done_count == 0:
        overall = "not_started"
    elif done_count == len(GOLD_MODULES):
        overall = "success"
    else:
        overall = "partial"
    return modules, overall


@router.get("/status/{doc_id}", response_model=PipelineStatus)
def get_status(doc_id: int):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT status_processing, error_message FROM silver.text WHERE doc_id = %s",
                (doc_id,),
            )
            silver_row = cur.fetchone()

            cur.execute(
                f"""SELECT {", ".join(GOLD_MODULES)}
                    FROM gold.analytics WHERE doc_id = %s""",
                (doc_id,),
            )
            gold_row = cur.fetchone()
    finally:
        conn.close()

    silver_status = silver_row[0] if silver_row else "not_started"
    silver_error = silver_row[1] if silver_row else None
    gold_modules, gold_status = _gold_status_for_row(gold_row)

    return PipelineStatus(
        doc_id=doc_id,
        bronze_status="success",  # a doc_id only exists once bronze's insert succeeded
        silver_status=silver_status,
        silver_error=silver_error,
        gold_modules=gold_modules,
        gold_status=gold_status,
    )


@router.get("/status", response_model=PipelineStatusResponse)
def get_all_statuses(limit: int = 100):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT b.doc_id, s.status_processing, s.error_message
                FROM bronze.documents b
                LEFT JOIN silver.text s ON s.doc_id = b.doc_id
                WHERE b.excluded = FALSE
                ORDER BY b.ingestion_date DESC
                LIMIT %s
                """,
                (limit,),
            )
            base_rows = cur.fetchall()

            doc_ids = [r[0] for r in base_rows]
            gold_by_doc = {}
            if doc_ids:
                cur.execute(
                    f"""SELECT doc_id, {", ".join(GOLD_MODULES)}
                        FROM gold.analytics WHERE doc_id = ANY(%s)""",
                    (doc_ids,),
                )
                for row in cur.fetchall():
                    gold_by_doc[row[0]] = row[1:]
    finally:
        conn.close()

    statuses = []
    for doc_id, status_processing, error_message in base_rows:
        gold_modules, gold_status = _gold_status_for_row(gold_by_doc.get(doc_id))
        statuses.append(PipelineStatus(
            doc_id=doc_id,
            bronze_status="success",
            silver_status=status_processing or "not_started",
            silver_error=error_message,
            gold_modules=gold_modules,
            gold_status=gold_status,
        ))

    return PipelineStatusResponse(statuses=statuses)