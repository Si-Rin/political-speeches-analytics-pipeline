import sys

from streamlit.testing.v1 import AppTest

sys.path.insert(0, "frontend")
import api_client  # noqa: E402

OVERVIEW = {"total_documents": 3, "by_source_type": [{"source_type": "audio", "count": 2}, {"source_type": "video", "count": 1}],
            "by_origin": [{"source_name": "youtube", "count": 3}], "date_min": "2025-01-01", "date_max": "2025-03-01",
            "silver_success": 2, "silver_failed": 1, "word_count_stats": {"min": 10, "max": 900, "mean": 400, "median": 350, "stddev": 100},
            "documents_over_time": [{"month": "2025-01", "count": 2}]}
CORPUS = {"documents_analyzed": 2, "total_words": 4000, "avg_ttr": 0.32, "avg_sentence_length": 15.5,
          "sentiment_scores": {"positive": 0.5}, "emotion_scores": {"joy": 0.4, "anger": 0.1},
          "label_distribution": [{"label": "economy", "count": 2, "pct": 100.0}], "pronoun_ratios": {"first_person_plural": 11.0},
          "top_words": [{"word": "great", "count": 40}], "top_bigrams": [{"phrase": "our country", "count": 9}], "top_trigrams": [],
          "top_repetitions": [], "oppositions": [{"pair": "freedom vs tyranny", "left": "freedom", "right": "tyranny", "left_count": 5, "right_count": 1}]}


def _patch(monkeypatch, overview=OVERVIEW, corpus=CORPUS):
    monkeypatch.setattr(api_client, "get_overview", lambda: overview)
    monkeypatch.setattr(api_client, "get_corpus_summary", lambda top_n=15: corpus)
    monkeypatch.setattr(api_client, "get_sentiment", lambda: {"distribution": [{"label": "positive", "count": 2, "pct": 100.0}], "over_time": []})
    monkeypatch.setattr(api_client, "get_emotions", lambda: {"distribution": [{"emotion": "joy", "count": 2, "pct": 100.0}]})
    monkeypatch.setattr(api_client, "get_entities", lambda top_n=15: {"persons": [{"text": "Biden", "count": 7}], "locations": [{"text": "America", "count": 12}], "groups": []})
    monkeypatch.setattr(api_client, "get_topics", lambda: {"topics": [{"corpus": "long_form", "topic_id": 0, "n_docs": 2, "keywords": ["tax", "jobs"], "is_outlier": False}], "validation": []})


def test_overview_is_the_default_landing_page_with_the_app_title(monkeypatch):
    _patch(monkeypatch)
    at = AppTest.from_file("frontend/app.py", default_timeout=30).run()
    assert not at.exception
    assert at.title[0].value == "Political Speeches Analytics"
    values = {m.label: m.value for m in at.metric}
    assert values["Dominant sentiment"] == "Positive" and values["Dominant emotion"] == "Joy"
    assert values["Top keyword"] == "great" and values["Most cited person"] == "Biden"


def test_overview_without_documents_shows_a_hint(monkeypatch):
    _patch(monkeypatch, overview={**OVERVIEW, "total_documents": 0})
    at = AppTest.from_file("frontend/app.py", default_timeout=30).run()
    assert not at.exception and any("No documents" in i.value for i in at.info)


def test_overview_before_gold_analysis(monkeypatch):
    _patch(monkeypatch, corpus={**CORPUS, "documents_analyzed": 0, "total_words": 0, "top_words": [], "label_distribution": []})
    at = AppTest.from_file("frontend/app.py", default_timeout=30).run()
    assert not at.exception and any("No Gold analysis yet" in i.value for i in at.info)
