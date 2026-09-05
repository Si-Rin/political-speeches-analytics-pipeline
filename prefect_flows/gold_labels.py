"""
Gold ingestion flow — Stage B (zero-shot multi-label classification).

This file does not contain any ML logic — it fetches pending documents, calls labels.classify_labels(transcript) (chunking -> model choice -> and score aggregation all live in that module) and upserts the result via the shared gold_db.upsert_gold_analytics() helper.
If the classification logic ever needs to change, this file should not need to change (it handles orchestration + persistence only).
"""
import argparse
from typing import Optional

from prefect import flow, task, get_run_logger

from prefect_flows.analytics.labels import classify_labels
from prefect_flows.analytics.gold_db import upsert_gold_analytics
from prefect_flows.clients import get_postgres_connection


@task
def get_pending_documents(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None) -> list[dict]:
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            base = """
                SELECT s.doc_id, s.transcript
                FROM silver.text s
                LEFT JOIN gold.analytics g ON g.doc_id = s.doc_id
                WHERE s.status_processing = 'success'
                  AND s.transcript IS NOT NULL
                  AND (g.doc_id IS NULL OR g.labels IS NULL)
            """
            if doc_ids:
                cur.execute(base + " AND s.doc_id = ANY(%s)", (doc_ids,))
            elif limit:
                cur.execute(base + " ORDER BY s.doc_id LIMIT %s", (limit,))
            else:
                cur.execute(base + " ORDER BY s.doc_id")
            rows = cur.fetchall()
            return [{"doc_id": r[0], "transcript": r[1]} for r in rows]
    finally:
        conn.close()


@flow(name="gold-labels")
def ingest_gold_labels(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None):
    logger = get_run_logger()
    documents = get_pending_documents(limit=limit, doc_ids=doc_ids)
    logger.info(f"Computing labels for {len(documents)} document(s)")

    processed = 0
    for doc_row in documents:
        transcript = doc_row["transcript"]

        labels = classify_labels(transcript)

        upsert_gold_analytics(
            doc_row["doc_id"],
            labels=labels,
        )
        processed += 1
        logger.info(
            f"doc_id={doc_row['doc_id']}: labels={labels['labels']}"
        )

    logger.info(f"Gold labels complete: {processed} processed")
    return {"processed": processed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Process only N pending documents")
    parser.add_argument("--doc-ids", type=int, nargs="+", help="Process specific doc_id(s) only")
    args = parser.parse_args()
    ingest_gold_labels(limit=args.limit, doc_ids=args.doc_ids)