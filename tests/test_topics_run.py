import pytest
from fastapi.testclient import TestClient

from backend import pipeline_runner
from backend.main import app
from backend.routes import topics_run

client = TestClient(app)


def _constant(path: str, name: str) -> int:
    """Read a module-level integer constant from source (the flow modules need bertopic/torch to import)."""
    import re
    from pathlib import Path
    return int(re.search(rf"^{name}\s*=\s*(\d+)", Path(path).read_text(), re.M).group(1))


def test_thresholds_match_the_flow():
    assert topics_run.SHORT_FORM_WORD_COUNT_THRESHOLD == _constant("prefect_flows/gold_topics.py", "SHORT_FORM_WORD_COUNT_THRESHOLD")
    assert topics_run.MIN_DOCS_FOR_TOPIC_MODELING == _constant("prefect_flows/analytics/topics.py", "MIN_DOCS_FOR_TOPIC_MODELING")


@pytest.mark.parametrize("prefect,expected", [("SCHEDULED", "queued"), ("PENDING", "queued"), ("RUNNING", "running"), ("COMPLETED", "completed"), ("FAILED", "failed"), ("CRASHED", "failed"), ("CANCELLED", "cancelled"), (None, "idle")])
def test_state_mapping(prefect, expected):
    assert topics_run.map_state(prefect) == expected


def _setup(monkeypatch, long_n, short_n, last=None, created=None):
    monkeypatch.setattr(topics_run, "_eligible_counts", lambda: (long_n, short_n))
    monkeypatch.setattr(pipeline_runner, "latest_topics_flow_run", lambda: last)
    monkeypatch.setattr(pipeline_runner, "create_topics_flow_run", lambda: (created.append(1) or "fr-1") if created is not None else "fr-1")


def test_status_reports_eligibility_and_last_run(monkeypatch):
    _setup(monkeypatch, 25, 3, {"state_type": "FAILED", "state_message": "boom", "start_time": "t0", "end_time": "t1"})
    body = client.get("/analytics/topics/run").json()
    assert body["can_run"] is True and body["state"] == "failed" and body["message"] == "boom" and body["min_docs"] == 20


def test_start_is_refused_without_enough_documents(monkeypatch):
    created = []
    _setup(monkeypatch, 4, 2, created=created)
    r = client.post("/analytics/topics/run")
    assert r.status_code == 422 and "at least 20" in r.json()["detail"] and not created


def test_start_is_refused_while_a_run_is_active(monkeypatch):
    created = []
    _setup(monkeypatch, 30, 0, {"state_type": "RUNNING"}, created)
    assert client.post("/analytics/topics/run").status_code == 409 and not created


def test_start_submits_the_flow(monkeypatch):
    created = []
    _setup(monkeypatch, 30, 0, {"state_type": "COMPLETED"}, created)
    r = client.post("/analytics/topics/run")
    assert r.status_code == 202 and r.json()["state"] == "queued" and created == [1]


def test_prefect_down_is_a_502(monkeypatch):
    _setup(monkeypatch, 30, 0)
    def boom(): raise pipeline_runner.PrefectError("down")
    monkeypatch.setattr(pipeline_runner, "create_topics_flow_run", boom)
    assert client.post("/analytics/topics/run").status_code == 502
