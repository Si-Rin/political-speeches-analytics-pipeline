"""
Gold ingestion flow — Stage C (unsupervised topic modeling).

Unlike every other Gold flow, this one is NOT a per-document loop, this flow fetches every eligible document at once and fits BERTopic over it.
Re-running this flow re-fits over the ENTIRE corpus (not just newly-added docs) and can reassign topic_ids for documents that were already processed in a previous run

GENRE SPLIT: the corpus mixes full speech transcripts (hundreds to thousands of words) with tweets/truths (tens of words).
Fitting one BERTopic model over both let document LENGTH dominate the embedding space over actual rhetorical content — the model cleanly separated "tweets"/"truths" from "speeches".
Sentence embeddings are known to be sensitive to length/register this way, and it doesn't self-correct with min_topic_size or UMAP tuning since the dominant axis of variation genuinely is length, not content.

Splitting the corpus by word count into two independent fits — long-form and short-form — lets each register's model find topical structure within itself instead of across the length gap.
This also matches standard practice in political-discourse NLP: social-media text and long-form speech are usually modeled separately because they differ in discourse structure, not just length.
"""
import argparse
from typing import Optional

from prefect import flow, task, get_run_logger

from prefect_flows.analytics.topics import fit_topics, MIN_DOCS_FOR_TOPIC_MODELING
from prefect_flows.analytics.gold_db import upsert_gold_analytics
from prefect_flows.clients import get_postgres_connection

# Below this word count, a document is treated as "short-form" (brief statements) and modeled separately from full transcripts
SHORT_FORM_WORD_COUNT_THRESHOLD = 500


@task
def get_all_documents(doc_ids: Optional[list[int]] = None) -> list[dict]:
    """
    Unlike gold_linguistic.py/gold_sentiment.py/gold_labels.py, this does NOT filter out documents that already have `topics` populated
    A corpus-wide refit needs every eligible document every time, since adding new documents can shift topic boundaries for old ones too.
    """
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            base = """
                SELECT s.doc_id, s.transcript, s.word_count
                FROM silver.text s
                WHERE s.status_processing = 'success'
                  AND s.transcript IS NOT NULL
            """
            if doc_ids:
                cur.execute(base + " AND s.doc_id = ANY(%s) ORDER BY s.doc_id", (doc_ids,))
            else:
                cur.execute(base + " ORDER BY s.doc_id")
            rows = cur.fetchall()
            return [{"doc_id": r[0], "transcript": r[1], "word_count": r[2]} for r in rows]
    finally:
        conn.close()


def _fit_and_upsert(corpus_label: str, bucket: list[dict], logger) -> tuple[int, int]:
    """Fits one BERTopic model over bucket and upserts results, tagging each document's topics JSONB with which corpus it was modeled against (long/short-form)."""
    if len(bucket) < MIN_DOCS_FOR_TOPIC_MODELING:
        logger.warning(
            f"{corpus_label}: only {len(bucket)} document(s) available "
            f"(need >= {MIN_DOCS_FOR_TOPIC_MODELING}); skipping for now."
        )
        return 0, 0

    ids = [d["doc_id"] for d in bucket]
    texts = [d["transcript"] for d in bucket]
    results = fit_topics(ids, texts)

    for doc_id, result in results.items():
        upsert_gold_analytics(
            doc_id,
            topics={
                "corpus": corpus_label,
                "topic_id": result["topic_id"],
                "topic_keywords": result["topic_keywords"],
                "probability": result["probability"],
            },
            keywords=result["topic_keywords"],
        )

    n_topics = len({r["topic_id"] for r in results.values() if r["topic_id"] != -1})
    logger.info(f"{corpus_label}: {len(results)} processed, {n_topics} topic(s) found")
    return len(results), n_topics


@flow(name="gold-topics")
def ingest_gold_topics(doc_ids: Optional[list[int]] = None):
    logger = get_run_logger()
    documents = get_all_documents(doc_ids=doc_ids)
    logger.info(f"{len(documents)} document(s) available for topic modeling")

    long_form = [d for d in documents if (d["word_count"] or 0) >= SHORT_FORM_WORD_COUNT_THRESHOLD]
    short_form = [d for d in documents if (d["word_count"] or 0) < SHORT_FORM_WORD_COUNT_THRESHOLD]

    long_processed, long_topics = _fit_and_upsert("long_form", long_form, logger)
    short_processed, short_topics = _fit_and_upsert("short_form", short_form, logger)

    total_processed = long_processed + short_processed
    total_topics = long_topics + short_topics
    logger.info(f"Gold topics complete: {total_processed} processed, {total_topics} topic(s) found total")
    return {
        "processed": total_processed,
        "topics_found": total_topics,
        "long_form": {"processed": long_processed, "topics_found": long_topics},
        "short_form": {"processed": short_processed, "topics_found": short_topics},
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--doc-ids", type=int, nargs="+",
        help="Restrict corpus to specific doc_id(s) — the model still fits fresh over just those",
    )
    args = parser.parse_args()
    ingest_gold_topics(doc_ids=args.doc_ids)