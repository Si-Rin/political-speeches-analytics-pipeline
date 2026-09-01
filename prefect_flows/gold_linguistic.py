"""
Gold ingestion flow — Stage A (spaCy-based linguistic metrics)

For each silver.text row with status_processing='success' and no lex_metrics or syntactic_metrics computed yet, parses the transcript once with the shared spaCy pipeline and runs every Stage-A module on that same Doc 
— currently lexical.compute_lexical_metrics() and syntactic.compute_syntactic_metrics() — then upserts both into their own gold.analytics columns via a single dynamic UPDATE SET (not a full row replace), so Stage B/C modules can populate their own columns on the same row without clobbering each other.
 
Note on the shared Doc: any future Stage-A module (NER — doc.ents is already populated by the same parse, no extra model needed) should be plugged into this same per-document loop, called on `doc` right alongside the calls below — rather than becoming its own flow that re-fetches the transcript and re-parses it from scratch. 
Stage B (sentiment/emotion) and Stage C (topics) don't depend on this Doc (see nlp_pipeline.py) and should stay separate flows.
"""
import argparse
from typing import Optional
from psycopg2.extras import Json

from prefect import flow, task, get_run_logger

from prefect_flows.analytics.entities import extract_entities
from prefect_flows.analytics.gold_db import upsert_gold_analytics
from prefect_flows.analytics.lexical import compute_lexical_metrics
from prefect_flows.analytics.syntactic import compute_syntactic_metrics
from prefect_flows.analytics.nlp_pipeline import get_doc
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
                  AND (g.doc_id IS NULL OR g.lex_metrics IS NULL OR g.syntactic_metrics IS NULL OR g.entities IS NULL)
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


@flow(name="gold-linguistic-metrics")
def ingest_gold_linguistic(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None):
    logger = get_run_logger()
    documents = get_pending_documents(limit=limit, doc_ids=doc_ids)
    logger.info(f"Computing linguistic metrics for {len(documents)} document(s)")
 
    processed = 0
    for doc_row in documents:
        doc = get_doc(doc_row["transcript"])  # parsed once, reused below
 
        lex_metrics = compute_lexical_metrics(doc)
        syntactic_metrics = compute_syntactic_metrics(doc)
        entities = extract_entities(doc)
 
        upsert_gold_analytics(
            doc_row["doc_id"],
            lex_metrics=lex_metrics,
            syntactic_metrics=syntactic_metrics,
            entities=entities,
        )
        processed += 1
        logger.info(
            f"doc_id={doc_row['doc_id']}: {lex_metrics['total_words']} words, "
            f"{len(lex_metrics['top_content_words'])} top content words, "
            f"{syntactic_metrics['speech_stats']['sentence_count']} sentences,"
            f"{entities['total_entities']} entities"
        )
 
    logger.info(f"Gold linguistic metrics complete: {processed} processed")
    return {"processed": processed}
 
 
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Process only N pending documents")
    parser.add_argument("--doc-ids", type=int, nargs="+", help="Process specific doc_id(s) only")
    args = parser.parse_args()
    ingest_gold_linguistic(limit=args.limit, doc_ids=args.doc_ids)