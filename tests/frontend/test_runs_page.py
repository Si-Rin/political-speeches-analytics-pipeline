import sys

from streamlit.testing.v1 import AppTest

sys.path.insert(0, "frontend")
import api_client  # noqa: E402

RUN = {"run_id": "abcdef123456", "source_name": "youtube", "status": "completed", "requested_at": "2026-10-18T08:24:09", "documents_ingested": 1,
       "mode": "single", "dry_run": False}
DETAIL = {**RUN, "listing_pages_visited": 0, "error_message": None, "counts": {"ingested": 1, "fetch_failed": 2, "known_url_skipped": 1},
          "reasons": {"format_unavailable": 2}}


def test_outcomes_are_a_chart_and_tables_not_json(monkeypatch):
    monkeypatch.setattr(api_client, "list_runs", lambda limit=50, status=None: [RUN])
    monkeypatch.setattr(api_client, "get_run", lambda run_id: DETAIL)
    monkeypatch.setattr(api_client, "get_run_log", lambda *a, **k: {"items": [], "total": 0})
    at = AppTest.from_file("frontend/pages/runs.py", default_timeout=30).run()
    assert not at.exception
    assert not at.json                                          # no raw JSON
    assert {m.label for m in at.metric} >= {"Ingested", "Download failed", "Already collected"}
