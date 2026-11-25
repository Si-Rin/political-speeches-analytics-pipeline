import sys

import pytest
from streamlit.testing.v1 import AppTest

sys.path.insert(0, "frontend")
import api_client  # noqa: E402


def mod(name, result): return {"module": name, "done": result is not None, "result": result}
full = [
 mod("lexical", {"pronoun_ratios": {"first_person_plural": 12.3, "second_person": 5.1}, "top_content_words": [{"word": "great", "count": 40}, {"word": "country", "count": 22}],
   "repetitions": [{"phrase": "we will build", "count": 4}], "oppositions": [{"pair": ["freedom", "tyranny"], "counts": [5, 1]}],
   "bigrams": [{"phrase": "our country", "count": 9}], "trigrams": [{"phrase": "make america great", "count": 6}],
   "lexical_diversity": {"ttr": 0.31, "mtld": 62.4}, "total_words": 3200}),
 mod("syntactic", {"speech_stats": {"sentence_count": 210, "avg_sentence_length": 15.2, "question_count": 3}, "modality": {"obligation_modality": 3.1, "possibility_modality": 4.0},
   "pos_distribution": {"noun": 22.0, "verb": 18.5}, "verb_tense": {"past": 20.0, "present": 60.0, "future": 15.0, "non_finite": 5.0}}),
 mod("entities", {"persons": [{"text": "Biden", "count": 7}], "locations": [{"text": "America", "count": 12}], "by_label": {"PERSON": 7, "GPE": 12}, "total_entities": 19}),
 mod("sentiment", {"label": "positive", "scores": {"positive": 0.6, "negative": 0.1, "neutral": 0.3}}),
 mod("emotions", {"dominant_emotion": "joy", "scores": {"joy": 0.5, "anger": 0.1}}),
 mod("labels", {"scores": {"economy": 0.8, "security": 0.2}, "labels": ["economy"], "threshold": 0.5}),
]
empty = [mod(n, None) for n in ["lexical","syntactic","entities","sentiment","emotions","labels"]]

STATUSES = [
  {"doc_id": 2, "bronze_status": "success", "silver_status": "success", "silver_error": None, "gold_modules": full, "gold_status": "success"},
  {"doc_id": 1, "bronze_status": "success", "silver_status": "failed", "silver_error": "boom", "gold_modules": empty, "gold_status": "not_started"}]


def test_documents_page_renders_without_json_dump(monkeypatch):
    monkeypatch.setattr(api_client, "get_all_statuses", lambda: STATUSES)
    monkeypatch.setattr(api_client, "get_history", lambda *a, **k: [{"doc_id": 2, "title": "Speech A", "source": "x"}])
    at = AppTest.from_file("frontend/pages/documents.py", default_timeout=30).run()
    assert not at.exception
    assert len(at.tabs) == 6                      # per-document modules only: no topics/keywords tabs
    assert not at.json                            # no raw st.json blocks
    at.selectbox[0].set_value(1).run()
    assert not at.exception and any("boom" in e.value for e in at.error)
