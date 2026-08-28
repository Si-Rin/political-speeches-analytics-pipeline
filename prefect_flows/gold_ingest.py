"""
Gold ingestion flow — lexical/rhetorical metrics.

For each silver.text row with status_processing='success' and no
lex_metrics computed yet, runs compute_lex_metrics() and upserts into
gold.analytics.lex_metrics. Uses a targeted UPDATE SET (not a full row
replace) so later Gold modules (NER, sentiment, topics) can populate
their own columns on the same row without clobbering each other.
"""
import argparse
from typing import Optional

from prefect import flow, task, get_run_logger

from prefect_flows.analytics.lexical_metrics import compute_lex_metrics
from prefect_flows.clients import get_pg_conn


@task
def get_pending_documents(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None) -> list[dict]:
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            base = """
                SELECT s.doc_id, s.transcript
                FROM silver.text s
                LEFT JOIN gold.analytics g ON g.doc_id = s.doc_id
                WHERE s.status_processing = 'success'
                  AND s.transcript IS NOT NULL
                  AND (g.doc_id IS NULL OR g.lex_metrics IS NULL)
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


@task
def upsert_lex_metrics(doc_id: int, lex_metrics: dict):
    from psycopg2.extras import Json
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO gold.analytics (doc_id, lex_metrics, analysis_date)
                VALUES (%s, %s, now())
                ON CONFLICT (doc_id) DO UPDATE SET
                    lex_metrics = EXCLUDED.lex_metrics,
                    analysis_date = now()
                """,
                (doc_id, Json(lex_metrics)),
            )
        conn.commit()
    finally:
        conn.close()


@flow(name="gold-lexical-metrics")
def ingest_gold_lexical(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None):
    logger = get_run_logger()
    documents = get_pending_documents(limit=limit, doc_ids=doc_ids)
    logger.info(f"Computing lexical metrics for {len(documents)} document(s)")

    processed = 0
    for doc in documents:
        metrics = compute_lex_metrics(doc["transcript"])
        upsert_lex_metrics(doc["doc_id"], metrics)
        processed += 1
        logger.info(f"doc_id={doc['doc_id']}: {metrics['total_words']} words, "
                     f"{len(metrics['buzzwords'])} buzzwords, {len(metrics['repetitions'])} repeated openings")

    logger.info(f"Gold lexical metrics complete: {processed} processed")
    return {"processed": processed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Process only N pending documents")
    parser.add_argument("--doc-ids", type=int, nargs="+", help="Process specific doc_id(s) only")
    args = parser.parse_args()
    ingest_gold_lexical(limit=args.limit, doc_ids=args.doc_ids)