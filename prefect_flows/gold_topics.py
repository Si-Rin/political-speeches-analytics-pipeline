"""
Gold ingestion flow — Stage C (unsupervised topic modeling).

Clustering, embedding, and c-TF-IDF keyword extraction all live in topics.py/embeddings.py.

Unlike every other Gold flow, this one is NOT a per-document loop: topics -> relative to the whole corpus, this flow fetches every document at once, fits a single BERTopic model over the full corpus, and then upserts each document's resulting topic/keywords individually.
Re-running this flow re-fits over the ENTIRE corpus (not just newly-added docs) and can reassign topic_ids for documents that were already processed in a previous run.
"""
import argparse
from typing import Optional

from prefect import flow, task, get_run_logger

from prefect_flows.analytics.topics import fit_topics, MIN_DOCS_FOR_TOPIC_MODELING
from prefect_flows.analytics.gold_db import upsert_gold_analytics
from prefect_flows.clients import get_postgres_connection


@task
def get_all_documents(doc_ids: Optional[list[int]] = None) -> list[dict]:
    """
    Unlike gold_linguistic.py/gold_sentiment.py/gold_labels.py, this does NOT filter out documents that already have `topics` populated
    A corpus-wide refit needs every eligible document every time since adding new documents can shift topic boundaries for old ones too.
    """
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            base = """
                SELECT s.doc_id, s.transcript
                FROM silver.text s
                WHERE s.status_processing = 'success'
                  AND s.transcript IS NOT NULL
            """
            if doc_ids:
                cur.execute(base + " AND s.doc_id = ANY(%s) ORDER BY s.doc_id", (doc_ids,))
            else:
                cur.execute(base + " ORDER BY s.doc_id", )
            rows = cur.fetchall()
            return [{"doc_id": r[0], "transcript": r[1]} for r in rows]
    finally:
        conn.close()


@flow(name="gold-topics")
def ingest_gold_topics(doc_ids: Optional[list[int]] = None):
    logger = get_run_logger()
    documents = get_all_documents(doc_ids=doc_ids)
    logger.info(f"Fitting topic model over {len(documents)} document(s)")

    if len(documents) < MIN_DOCS_FOR_TOPIC_MODELING:
        logger.warning(
            f"Only {len(documents)} document(s) available "
            f"(need >= {MIN_DOCS_FOR_TOPIC_MODELING}); skipping topic modeling for now."
        )
        return {"processed": 0}

    ids = [d["doc_id"] for d in documents]
    texts = [d["transcript"] for d in documents]

    per_doc_results = fit_topics(ids, texts)

    processed = 0
    for doc_id, result in per_doc_results.items():
        upsert_gold_analytics(
            doc_id,
            topics={"topic_id": result["topic_id"], "probability": result["probability"]},
            keywords=result["topic_keywords"],
        )
        processed += 1

    n_topics = len({r["topic_id"] for r in per_doc_results.values() if r["topic_id"] != -1})
    logger.info(f"Gold topics complete: {processed} processed, {n_topics} topic(s) found")
    return {"processed": processed, "topics_found": n_topics}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--doc-ids", type=int, nargs="+",
        help="Restrict corpus to specific doc_id(s) — the model still fits fresh over just those",
    )
    args = parser.parse_args()
    ingest_gold_topics(doc_ids=args.doc_ids)