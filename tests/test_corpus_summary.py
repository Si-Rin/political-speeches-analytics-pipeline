from backend.routes.analytics import aggregate_corpus

LEX = {"pronoun_ratios": {"first_person_plural": 10.0}, "top_content_words": [{"word": "great", "count": 5}, {"word": "jobs", "count": 2}],
       "repetitions": [{"phrase": "we will build", "count": 3}], "oppositions": [{"pair": ["freedom", "tyranny"], "counts": [2, 1]}],
       "bigrams": [{"phrase": "our country", "count": 4}], "trigrams": [], "lexical_diversity": {"ttr": 0.4, "mtld": None}, "total_words": 1000}


def test_aggregates_sum_counts_and_average_scores():
    rows = [
        ({"label": "positive", "scores": {"positive": 0.8, "negative": 0.2}}, {"dominant_emotion": "joy", "scores": {"joy": 0.6}},
         {"scores": {"economy": 0.9, "security": 0.1}}, LEX, {"speech_stats": {"avg_sentence_length": 12.0}}),
        ({"label": "negative", "scores": {"positive": 0.2, "negative": 0.8}}, None, {"scores": {"security": 0.7, "economy": 0.3}},
         {**LEX, "total_words": 500, "lexical_diversity": {"ttr": 0.6}}, {"speech_stats": {"avg_sentence_length": 18.0}}),
    ]
    r = aggregate_corpus(rows)
    assert r.documents_analyzed == 2 and r.total_words == 1500
    assert r.top_words[0].word == "great" and r.top_words[0].count == 10
    assert r.sentiment_scores == {"positive": 0.5, "negative": 0.5}
    assert r.emotion_scores == {"joy": 0.6}
    assert r.avg_ttr == 0.5 and r.avg_sentence_length == 15.0
    assert {l.label: l.count for l in r.label_distribution} == {"economy": 1, "security": 1}
    assert r.oppositions[0].left_count == 4 and r.oppositions[0].right_count == 2


def test_empty_corpus_is_safe():
    r = aggregate_corpus([])
    assert r.documents_analyzed == 0 and r.top_words == [] and r.avg_ttr is None


def test_document_without_any_gold_result_is_not_counted():
    assert aggregate_corpus([(None, None, None, None, None)]).documents_analyzed == 0
