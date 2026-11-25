import sys

from streamlit.testing.v1 import AppTest

sys.path.insert(0, "frontend")
import api_client  # noqa: E402


def _patch(monkeypatch, status, started=None):
    monkeypatch.setattr(api_client, "get_topics_run_status", lambda: status)
    monkeypatch.setattr(api_client, "start_topics_run", lambda: (started.append(1) or status) if started is not None else None)
    monkeypatch.setattr(api_client, "get_topics", lambda: {"topics": [], "validation": []})
    monkeypatch.setattr(api_client, "get_labels", lambda: None)


BASE = {"long_form_docs": 3, "short_form_docs": 1, "min_docs": 20, "can_run": False, "state": "idle", "message": None, "started_at": None, "finished_at": None}


def test_button_disabled_and_explained_when_not_enough_documents(monkeypatch):
    _patch(monkeypatch, BASE)
    at = AppTest.from_file("frontend/pages/topics_labels.py", default_timeout=30).run()
    assert not at.exception
    assert next(b for b in at.button if b.label == "Compute topics now").disabled
    assert any("Not enough documents" in w.value for w in at.warning)


def test_button_starts_the_run_when_enough_documents(monkeypatch):
    started = []
    _patch(monkeypatch, {**BASE, "long_form_docs": 25, "can_run": True}, started)
    at = AppTest.from_file("frontend/pages/topics_labels.py", default_timeout=30).run()
    btn = next(b for b in at.button if b.label == "Compute topics now")
    assert not btn.disabled
    btn.click().run()
    assert started == [1]


def test_button_disabled_while_running(monkeypatch):
    _patch(monkeypatch, {**BASE, "long_form_docs": 25, "can_run": True, "state": "running"})
    at = AppTest.from_file("frontend/pages/topics_labels.py", default_timeout=30).run()
    assert next(b for b in at.button if b.label == "Compute topics now").disabled
    assert any("in progress" in i.value for i in at.info)
