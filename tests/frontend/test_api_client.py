"""api_client against a fake backend (requests is monkeypatched, never a real HTTP call)."""
import json

import pytest
import requests

import api_client


class FakeResponse:
    def __init__(self, status_code=200, json_body=None, text=""):
        self.status_code, self._json, self.text = status_code, json_body, text or json.dumps(json_body or {})

    def json(self):
        if self._json is None:
            raise ValueError("no json")
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(response=self)


@pytest.fixture
def fake(monkeypatch):
    calls = {}

    def make(method):
        def call(url, *a, **kw):
            calls[method] = calls.get(method, []) + [(url, kw)]
            return calls["_next"].pop(0)
        return call

    calls["_next"] = []
    monkeypatch.setattr(requests, "get", make("get"))
    monkeypatch.setattr(requests, "post", make("post"))
    return calls


def queue(fake, response):
    fake["_next"].append(response)


def test_get_returns_json_on_success(fake):
    queue(fake, FakeResponse(200, {"a": 1}))
    assert api_client._get("/x") == {"a": 1}


def test_get_shows_error_and_returns_none_on_http_error(fake, monkeypatch):
    errors = []
    monkeypatch.setattr(__import__("streamlit"), "error", lambda msg: errors.append(msg))
    queue(fake, FakeResponse(404, {"detail": "not found"}))
    assert api_client._get("/x") is None
    assert "not found" in errors[0]


def test_get_drops_none_query_params(fake):
    queue(fake, FakeResponse(200, {}))
    api_client._get("/x", status=None, limit=5)
    _, kwargs = fake["get"][0]
    assert kwargs["params"] == {"limit": 5}


def test_get_shows_network_error(fake, monkeypatch):
    errors = []
    monkeypatch.setattr(__import__("streamlit"), "error", lambda msg: errors.append(msg))

    def boom(*a, **kw):
        raise requests.ConnectionError("refused")
    monkeypatch.setattr(requests, "get", boom)
    assert api_client._get("/x") is None
    assert "Could not reach the backend" in errors[0]


def test_create_run_success_includes_dry_run_and_drops_none_fields(fake):
    queue(fake, FakeResponse(202, {"run_id": "r1", "status": "queued"}))
    result = api_client.create_run("direct_url", url="https://h/a.mp3", content_type="audio", title=None, dry_run=True)
    assert result == {"run_id": "r1", "status": "queued"}
    _, kwargs = fake["post"][0]
    assert kwargs["json"] == {"source": "direct_url", "dry_run": True, "url": "https://h/a.mp3", "content_type": "audio"}


def test_create_run_stringifies_dates(fake):
    import datetime
    queue(fake, FakeResponse(202, {"run_id": "r1"}))
    api_client.create_run("direct_url", url="u", content_type="text", publication_date=datetime.date(2017, 1, 20))
    _, kwargs = fake["post"][0]
    assert kwargs["json"]["publication_date"] == "2017-01-20"


def test_create_run_422_shows_error_and_returns_none(fake, monkeypatch):
    errors = []
    monkeypatch.setattr(__import__("streamlit"), "error", lambda msg: errors.append(msg))
    queue(fake, FakeResponse(422, {"detail": "bad url"}))
    assert api_client.create_run("direct_url", url="bad", content_type="text") is None
    assert "bad url" in errors[0]


def test_create_run_502_reports_prefect_outage_but_returns_the_failed_run(fake, monkeypatch):
    errors = []
    monkeypatch.setattr(__import__("streamlit"), "error", lambda msg: errors.append(msg))
    queue(fake, FakeResponse(502, {"detail": {"run_id": "r1", "message": "Could not reach Prefect Server"}}))
    result = api_client.create_run("direct_url", url="u", content_type="text")
    assert result == {"run_id": "r1", "status": "failed"}
    assert "Prefect" in errors[0]


def test_create_run_network_error(fake, monkeypatch):
    errors = []
    monkeypatch.setattr(__import__("streamlit"), "error", lambda msg: errors.append(msg))

    def boom(*a, **kw):
        raise requests.ConnectionError("refused")
    monkeypatch.setattr(requests, "post", boom)
    assert api_client.create_run("direct_url", url="u", content_type="text") is None
    assert "Could not reach the backend" in errors[0]


def test_upload_file_posts_multipart_and_returns_json(fake):
    queue(fake, FakeResponse(201, {"staging_key": "u/a.mp3", "file_name": "a.mp3", "size_bytes": 3}))
    result = api_client.upload_file("a.mp3", b"AAA", "audio/mpeg")
    assert result["staging_key"] == "u/a.mp3"
    _, kwargs = fake["post"][0]
    assert kwargs["files"]["file"][0] == "a.mp3" and kwargs["files"]["file"][1] == b"AAA"


def test_upload_file_error(fake, monkeypatch):
    errors = []
    monkeypatch.setattr(__import__("streamlit"), "error", lambda msg: errors.append(msg))
    queue(fake, FakeResponse(413, {"detail": "too large"}))
    assert api_client.upload_file("a.mp3", b"AAA", "audio/mpeg") is None
    assert "too large" in errors[0]


def test_list_runs_unwraps_and_defaults_to_empty(fake):
    queue(fake, FakeResponse(200, {"runs": [{"run_id": "r1"}]}))
    assert api_client.list_runs() == [{"run_id": "r1"}]
    queue(fake, FakeResponse(500, {}))
    assert api_client.list_runs() == []


def test_get_history_maps_all_to_no_filter(fake):
    queue(fake, FakeResponse(200, {"documents": []}))
    api_client.get_history(source_type="All")
    _, kwargs = fake["get"][0]
    assert kwargs["params"] == {}
