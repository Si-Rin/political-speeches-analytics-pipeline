import sys

from streamlit.testing.v1 import AppTest

sys.path.insert(0, "frontend")
import api_client  # noqa: E402

RUN = {"run_id": "abcdef123456", "source_name": "youtube", "status": "failed", "requested_at": "2026-10-18T08:24:09", "documents_ingested": 1, "mode": "single", "dry_run": False}
DETAIL = {**RUN, "listing_pages_visited": 0, "error_message": "boom", "counts": {"ingested": 1, "fetch_failed": 2, "known_url_skipped": 1}, "reasons": {"format_unavailable": 2}, "started_at": "2026-10-18T08:24:10Z", "finished_at": "2026-10-18T08:26:10Z"}
LOG = {"items": [{"status": "ingested", "reason_code": None, "title_hint": "Speech", "source_url": "https://y", "detail": None, "doc_id": 7}], "total": 1}
STATUS = [{"doc_id": 7, "silver_status": "success", "silver_error": None, "bronze_status": "success", "gold_status": "partial", "gold_modules": [{"module": m, "done": i < 4, "result": None} for i, m in enumerate(["lexical", "syntactic", "entities", "sentiment", "emotions", "labels"])]}]


def _patch(monkeypatch, detail=DETAIL, retried=None):
    monkeypatch.setattr(api_client, "list_runs", lambda limit=50, status=None: [{**RUN, "status": detail["status"]}])
    monkeypatch.setattr(api_client, "get_run", lambda run_id: detail)
    monkeypatch.setattr(api_client, "get_run_log", lambda *a, **k: LOG)
    monkeypatch.setattr(api_client, "get_all_statuses", lambda: STATUS)
    monkeypatch.setattr(api_client, "retry_run", lambda run_id: (retried.append(run_id) or {"run_id": "new-run-1"}) if retried is not None else None)


def test_outcomes_duration_chain_and_no_json(monkeypatch):
    _patch(monkeypatch)
    at = AppTest.from_file("frontend/pages/runs.py", default_timeout=30).run()
    assert not at.exception and not at.json
    values = {m.label: m.value for m in at.metric}
    assert {"Ingested", "Download failed", "Already collected"} <= set(values)
    assert values["Duration"] == "2m 00s" and values["Average speed"] == "120 s / document"
    assert any("0 of 1 fully processed" in c.value for c in at.caption)      # Gold 4/6 -> not complete yet


def test_relaunch_button_only_for_failed_runs_and_selects_the_new_run(monkeypatch):
    retried = []
    _patch(monkeypatch, retried=retried)
    at = AppTest.from_file("frontend/pages/runs.py", default_timeout=30).run()
    btn = next(b for b in at.button if "Relaunch" in b.label)
    btn.click().run()
    assert retried == ["abcdef123456"] and not at.exception

    _patch(monkeypatch, detail={**DETAIL, "status": "completed"})
    at = AppTest.from_file("frontend/pages/runs.py", default_timeout=30).run()
    assert not any("Relaunch" in b.label for b in at.button)
    assert not any("Cancel" in b.label for b in at.button)


def test_active_run_offers_cancel_and_mentions_refresh(monkeypatch):
    _patch(monkeypatch, detail={**DETAIL, "status": "running", "finished_at": None, "error_message": None})
    at = AppTest.from_file("frontend/pages/runs.py", default_timeout=30).run()
    assert not at.exception and any("Cancel" in b.label for b in at.button)
    assert any("Refreshing every" in c.value for c in at.caption)
