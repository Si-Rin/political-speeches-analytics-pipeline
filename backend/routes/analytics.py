"""
Routes: Analytics dashboard (Overview / Topics & Labels / Sentiment & Entities).

Aggregation logic here mirrors what was built and verified for the
internship report's Chapter 4 (chapter4_stats.py, validate_topics.py) —
reused rather than reimplemented, so the dashboard and the report numbers
stay consistent with each other.

DB access is plain psycopg2 via prefect_flows.clients, same pattern as
routes/documents.py and routes/status.py.
"""
from collections import Counter, defaultdict
from typing import Optional

from fastapi import APIRouter

from prefect_flows.clients import get_postgres_connection

from backend.schemas import (
    OverviewResponse, SourceTypeCount, OriginCount, MonthCount, WordCountStats,
    TopicsResponse, TopicSummary, TopicValidation,
    LabelsResponse, LabelCount,
    SentimentResponse, SentimentCount, SentimentOverTime,
    EmotionsResponse, EmotionCount,
    EntitiesResponse, EntityCount,
)

router = APIRouter()


def _dominant_label(labels_json: Optional[dict]) -> Optional[str]:
    """Highest-scoring label for a document, regardless of whether it
    crossed the classification threshold. Same definition used in
    validate_topics.py — kept identical so dashboard and report numbers
    never silently diverge."""
    scores = (labels_json or {}).get("scores") or {}
    if not scores:
        return None
    return max(scores.items(), key=lambda kv: kv[1])[0]


# ---------------------------------------------------------------------------
# Overview
# ---------------------------------------------------------------------------
@router.get("/analytics/overview", response_model=OverviewResponse)
def get_overview():
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM bronze.documents")
            total = cur.fetchone()[0]

            cur.execute(
                "SELECT source_type, count(*) FROM bronze.documents GROUP BY 1 ORDER BY 2 DESC"
            )
            by_type = [SourceTypeCount(source_type=r[0], count=r[1]) for r in cur.fetchall()]

            cur.execute(
                """SELECT COALESCE(raw_metadata->>'source_name', 'unknown'), count(*)
                   FROM bronze.documents GROUP BY 1 ORDER BY 2 DESC"""
            )
            by_origin = [OriginCount(source_name=r[0], count=r[1]) for r in cur.fetchall()]

            cur.execute(
                "SELECT min(publication_date), max(publication_date) FROM silver.text WHERE publication_date IS NOT NULL"
            )
            date_min, date_max = cur.fetchone()

            cur.execute(
                """SELECT count(*) FILTER (WHERE status_processing = 'success'),
                          count(*) FILTER (WHERE status_processing = 'failed')
                   FROM silver.text"""
            )
            silver_success, silver_failed = cur.fetchone()

            cur.execute(
                """SELECT min(word_count), max(word_count), avg(word_count),
                          percentile_cont(0.5) WITHIN GROUP (ORDER BY word_count),
                          stddev(word_count)
                   FROM silver.text WHERE word_count IS NOT NULL"""
            )
            wc_row = cur.fetchone()
            word_count_stats = None
            if wc_row and wc_row[0] is not None:
                word_count_stats = WordCountStats(
                    min=wc_row[0],
                    max=wc_row[1],
                    mean=round(float(wc_row[2]), 1),
                    median=round(float(wc_row[3]), 1),
                    stddev=round(float(wc_row[4]), 1),
                )

            cur.execute(
                """SELECT to_char(publication_date, 'YYYY-MM') AS month, count(*)
                   FROM silver.text WHERE publication_date IS NOT NULL
                   GROUP BY 1 ORDER BY 1"""
            )
            documents_over_time = [MonthCount(month=r[0], count=r[1]) for r in cur.fetchall()]
    finally:
        conn.close()

    return OverviewResponse(
        total_documents=total,
        by_source_type=by_type,
        by_origin=by_origin,
        date_min=date_min,
        date_max=date_max,
        silver_success=silver_success or 0,
        silver_failed=silver_failed or 0,
        word_count_stats=word_count_stats,
        documents_over_time=documents_over_time,
    )


# ---------------------------------------------------------------------------
# Topics (+ cross-validation against the label taxonomy)
# ---------------------------------------------------------------------------
@router.get("/analytics/topics", response_model=TopicsResponse)
def get_topics():
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT topics, keywords, labels FROM gold.analytics WHERE topics IS NOT NULL"
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    groups: dict = {}
    corpus_baseline: dict = defaultdict(Counter)

    for topics, keywords, labels in rows:
        corpus = (topics or {}).get("corpus", "unknown")
        topic_id = (topics or {}).get("topic_id")
        key = (corpus, topic_id)
        if key not in groups:
            groups[key] = {"n_docs": 0, "keywords": keywords or [], "dominant_labels": []}
        groups[key]["n_docs"] += 1
        if not groups[key]["keywords"] and keywords:
            groups[key]["keywords"] = keywords

        dom = _dominant_label(labels)
        if dom:
            groups[key]["dominant_labels"].append(dom)
            corpus_baseline[corpus][dom] += 1

    topic_summaries = []
    validations = []
    for (corpus, topic_id), data in sorted(
        groups.items(), key=lambda kv: (kv[0][0], kv[0][1] if kv[0][1] is not None else -99)
    ):
        topic_summaries.append(TopicSummary(
            corpus=corpus,
            topic_id=topic_id,
            n_docs=data["n_docs"],
            keywords=data["keywords"] or [],
            is_outlier=(topic_id == -1),
        ))

        dom_labels = data["dominant_labels"]
        if dom_labels:
            counts = Counter(dom_labels)
            top_label, top_count = counts.most_common(1)[0]
            n = len(dom_labels)
            purity = top_count / n
            baseline_total = sum(corpus_baseline[corpus].values())
            baseline_rate = corpus_baseline[corpus][top_label] / baseline_total if baseline_total else 0
            lift = round(purity / baseline_rate, 2) if baseline_rate else None
            validations.append(TopicValidation(
                corpus=corpus,
                topic_id=topic_id,
                n_docs=n,
                dominant_label=top_label,
                purity=round(purity, 3),
                lift=lift,
            ))

    return TopicsResponse(topics=topic_summaries, validation=validations)


# ---------------------------------------------------------------------------
# Labels (zero-shot category distribution)
# ---------------------------------------------------------------------------
@router.get("/analytics/labels", response_model=LabelsResponse)
def get_labels():
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT labels, topics->>'corpus'
                   FROM gold.analytics WHERE labels IS NOT NULL AND topics IS NOT NULL"""
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    counters = {"long_form": Counter(), "short_form": Counter()}
    for labels, corpus in rows:
        if corpus not in counters:
            continue
        dom = _dominant_label(labels)
        if dom:
            counters[corpus][dom] += 1

    def _to_list(counter: Counter):
        total = sum(counter.values())
        if not total:
            return []
        return [
            LabelCount(label=label, count=count, pct=round(100 * count / total, 1))
            for label, count in counter.most_common()
        ]

    return LabelsResponse(
        long_form=_to_list(counters["long_form"]),
        short_form=_to_list(counters["short_form"]),
    )


# ---------------------------------------------------------------------------
# Sentiment
# ---------------------------------------------------------------------------
@router.get("/analytics/sentiment", response_model=SentimentResponse)
def get_sentiment():
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT sentiment->>'label', count(*)
                   FROM gold.analytics WHERE sentiment IS NOT NULL GROUP BY 1"""
            )
            dist_rows = cur.fetchall()

            cur.execute(
                """SELECT to_char(s.publication_date, 'YYYY-MM'), g.sentiment->>'label', count(*)
                   FROM gold.analytics g
                   JOIN silver.text s ON s.doc_id = g.doc_id
                   WHERE g.sentiment IS NOT NULL AND s.publication_date IS NOT NULL
                   GROUP BY 1, 2 ORDER BY 1"""
            )
            time_rows = cur.fetchall()
    finally:
        conn.close()

    total = sum(c for _, c in dist_rows)
    distribution = (
        [SentimentCount(label=l, count=c, pct=round(100 * c / total, 1)) for l, c in dist_rows]
        if total else []
    )

    by_month = defaultdict(lambda: {"positive": 0, "negative": 0, "neutral": 0})
    for month, label, count in time_rows:
        if label in by_month[month]:
            by_month[month][label] = count
    over_time = [SentimentOverTime(month=m, **v) for m, v in sorted(by_month.items())]

    return SentimentResponse(distribution=distribution, over_time=over_time)


# ---------------------------------------------------------------------------
# Emotions
# ---------------------------------------------------------------------------
@router.get("/analytics/emotions", response_model=EmotionsResponse)
def get_emotions():
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT emotions->>'dominant_emotion', count(*)
                   FROM gold.analytics WHERE emotions IS NOT NULL
                   GROUP BY 1 ORDER BY 2 DESC"""
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    total = sum(c for _, c in rows)
    distribution = (
        [EmotionCount(emotion=e, count=c, pct=round(100 * c / total, 1)) for e, c in rows]
        if total else []
    )
    return EmotionsResponse(distribution=distribution)


# ---------------------------------------------------------------------------
# Entities
# ---------------------------------------------------------------------------
@router.get("/analytics/entities", response_model=EntitiesResponse)
def get_entities(top_n: int = 15):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT entities FROM gold.analytics WHERE entities IS NOT NULL")
            rows = cur.fetchall()
    finally:
        conn.close()

    persons, locations, groups = Counter(), Counter(), Counter()
    for (entities,) in rows:
        entities = entities or {}
        for p in entities.get("persons", []):
            persons[p["text"]] += p.get("count", 1)
        for l in entities.get("locations", []):
            locations[l["text"]] += l.get("count", 1)
        for g in entities.get("groups", []):
            groups[g["text"]] += g.get("count", 1)

    def _top(counter: Counter):
        return [EntityCount(text=t, count=c) for t, c in counter.most_common(top_n)]

    return EntitiesResponse(persons=_top(persons), locations=_top(locations), groups=_top(groups))