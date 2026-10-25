"""
Routes: Analytics dashboard (Overview / Topics & Labels / Sentiment & Entities).

Aggregation logic here mirrors what was built and verified for the
internship report's Chapter 4 (chapter4_stats.py, validate_topics.py) —
reused rather than reimplemented, so the dashboard and the report numbers
stay consistent with each other.

DB access is plain psycopg2 via backend.db, same pattern as
routes/documents.py and routes/status.py.
"""
from collections import Counter, defaultdict
from typing import Optional

from fastapi import APIRouter

from backend.db import get_connection

from backend.schemas import (
    OverviewResponse, SourceTypeCount, OriginCount, MonthCount, WordCountStats,
    TopicsResponse, TopicSummary, TopicValidation,
    LabelsResponse, LabelCount,
    SentimentResponse, SentimentCount, SentimentOverTime,
    EmotionsResponse, EmotionCount,
    EntitiesResponse, EntityCount,
    CorpusSummaryResponse, WordCount, PhraseCount, OppositionTotal,
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
    conn = get_connection()
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
    conn = get_connection()
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
    conn = get_connection()
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
    conn = get_connection()
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
    conn = get_connection()
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
    conn = get_connection()
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


# ---------------------------------------------------------------------------
# Corpus summary
# ---------------------------------------------------------------------------
def _mean_scores(score_dicts) -> dict:
    """Mean of each key across documents (a document missing a key counts as 0 for it)."""
    score_dicts = [d for d in score_dicts if d]
    if not score_dicts:
        return {}
    keys = {k for d in score_dicts for k in d}
    return {k: round(sum(d.get(k, 0.0) for d in score_dicts) / len(score_dicts), 4) for k in keys}


def aggregate_corpus(rows, top_n: int = 15) -> CorpusSummaryResponse:
    """rows: iterable of (sentiment, emotions, labels, lex_metrics, syntactic_metrics) JSON dicts (any may be None).
    Pure function (no DB) so the aggregation is unit-testable."""
    rows = list(rows)
    words, bigrams, trigrams, reps = Counter(), Counter(), Counter(), Counter()
    opp = defaultdict(lambda: [0, 0])
    ttrs, sent_lengths, pronouns, label_counter = [], [], [], Counter()
    total_words, analysed = 0, 0

    for sentiment, emotions, labels, lex, syn in rows:
        lex, syn = lex or {}, syn or {}
        if sentiment or emotions or labels or lex or syn:
            analysed += 1
        total_words += int(lex.get("total_words") or 0)
        ttr = (lex.get("lexical_diversity") or {}).get("ttr")
        if ttr is not None:
            ttrs.append(ttr)
        if lex.get("pronoun_ratios"):
            pronouns.append(lex["pronoun_ratios"])
        for item in lex.get("top_content_words") or []:
            words[item["word"]] += item.get("count", 1)
        for item in lex.get("bigrams") or []:
            bigrams[item["phrase"]] += item.get("count", 1)
        for item in lex.get("trigrams") or []:
            trigrams[item["phrase"]] += item.get("count", 1)
        for item in lex.get("repetitions") or []:
            reps[item["phrase"]] += item.get("count", 1)
        for o in lex.get("oppositions") or []:
            left, right = o["pair"]
            opp[(left, right)][0] += o["counts"][0]
            opp[(left, right)][1] += o["counts"][1]
        asl = (syn.get("speech_stats") or {}).get("avg_sentence_length")
        if asl is not None:
            sent_lengths.append(asl)
        dom = _dominant_label(labels)
        if dom:
            label_counter[dom] += 1

    n_labels = sum(label_counter.values())
    return CorpusSummaryResponse(
        documents_analyzed=analysed,
        total_words=total_words,
        avg_ttr=round(sum(ttrs) / len(ttrs), 4) if ttrs else None,
        avg_sentence_length=round(sum(sent_lengths) / len(sent_lengths), 2) if sent_lengths else None,
        sentiment_scores=_mean_scores([(r[0] or {}).get("scores") for r in rows]),
        emotion_scores=_mean_scores([(r[1] or {}).get("scores") for r in rows]),
        label_distribution=[LabelCount(label=l, count=c, pct=round(100 * c / n_labels, 1)) for l, c in label_counter.most_common()],
        pronoun_ratios=_mean_scores(pronouns),
        top_words=[WordCount(word=w, count=c) for w, c in words.most_common(top_n)],
        top_bigrams=[PhraseCount(phrase=w, count=c) for w, c in bigrams.most_common(top_n)],
        top_trigrams=[PhraseCount(phrase=w, count=c) for w, c in trigrams.most_common(top_n)],
        top_repetitions=[PhraseCount(phrase=w, count=c) for w, c in reps.most_common(top_n)],
        oppositions=[OppositionTotal(pair=f"{l} vs {r}", left=l, right=r, left_count=c[0], right_count=c[1])
                     for (l, r), c in sorted(opp.items(), key=lambda kv: -(kv[1][0] + kv[1][1]))],
    )


@router.get("/analytics/corpus", response_model=CorpusSummaryResponse)
def get_corpus_summary(top_n: int = 15):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT g.sentiment, g.emotions, g.labels, g.lex_metrics, g.syntactic_metrics
                   FROM gold.analytics g JOIN bronze.documents b ON b.doc_id = g.doc_id
                   WHERE b.excluded = FALSE"""
            )
            rows = cur.fetchall()
    finally:
        conn.close()
    return aggregate_corpus(rows, top_n=top_n)
