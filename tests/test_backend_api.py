"""
API tests. Uploads and validation need nothing external; everything touching runs runs against a real Postgres that
already has init_schema.sql + updates.sql applied (skipped unless TEST_DATABASE_URL is set, like test_repo_postgres.py).
Prefect and MinIO are faked: the backend talks to them only through pipeline_runner and storage.
"""
import io
import os
import uuid

import pytest

pytest.importorskip("fastapi")
pytest.importorskip("multipart")
from fastapi.testclient import TestClient

from backend import db, pipeline_runner, runs_repo, storage
from backend.main import app
from backend.routes import uploads

DSN = os.environ.get("TEST_DATABASE_URL")
needs_db = pytest.mark.skipif(not DSN, reason="TEST_DATABASE_URL not set")


class FakePrefect:
    def __init__(self):
        self.created, self.cancelled, self.state, self.fail = [], [], "RUNNING", False

    def create_flow_run(self, run_id, source_name, params, params_version, dry_run):
        if self.fail:
            raise pipeline_runner.PrefectError("Could not reach Prefect Server")
        self.created.append(dict(run_id=run_id, source_name=source_name, params=params,
                                 params_version=params_version, dry_run=dry_run))
        return str(uuid.uuid4())

    def get_flow_run_state(self, flow_run_id):
        return self.state

    def cancel_flow_run(self, flow_run_id):
        self.cancelled.append(flow_run_id)


@pytest.fixture
def prefect(monkeypatch):
    fake = FakePrefect()
    monkeypatch.setattr(pipeline_runner, "create_flow_run", fake.create_flow_run)
    monkeypatch.setattr(pipeline_runner, "get_flow_run_state", fake.get_flow_run_state)
    monkeypatch.setattr(pipeline_runner, "cancel_flow_run", fake.cancel_flow_run)
    return fake


@pytest.fixture
def staged(monkeypatch):
    objects = {}
    monkeypatch.setattr(storage, "put_staging", lambda key, f, size, ct, client=None: objects.__setitem__(key, f.read()))
    monkeypatch.setattr(storage, "staging_exists", lambda key, client=None: key in objects)
    return objects


@pytest.fixture
def client(monkeypatch):
    import psycopg2
    if DSN:
        monkeypatch.setattr(db, "get_connection", lambda: psycopg2.connect(DSN))
    return TestClient(app)


@pytest.fixture
def cleanup():
    import psycopg2
    ids = []
    yield ids
    if DSN and ids:
        conn = psycopg2.connect(DSN)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM collection.runs WHERE run_id = ANY(%s::uuid[])", (ids,))
        conn.commit(); conn.close()


def direct_url(**extra):
    return {"source": "direct_url", "url": "https://h/a.mp3", "content_type": "audio", **extra}


# ---- uploads (no external service) ----------------------------------------------------------------

def test_upload_returns_a_staging_key_and_stores_the_bytes(client, staged):
    r = client.post("/uploads", files={"file": ("My Speech (final).mp3", io.BytesIO(b"AUDIO"), "audio/mpeg")})
    assert r.status_code == 201
    body = r.json()
    assert body["size_bytes"] == 5 and body["file_name"] == "My_Speech_final_.mp3"
    assert staged[body["staging_key"]] == b"AUDIO" and body["staging_key"].endswith("/My_Speech_final_.mp3")


def test_upload_rejects_empty_and_oversized_files(client, staged, monkeypatch):
    assert client.post("/uploads", files={"file": ("a.mp3", io.BytesIO(b""), "audio/mpeg")}).status_code == 422
    monkeypatch.setattr(uploads, "MAX_UPLOAD_BYTES", 3)
    assert client.post("/uploads", files={"file": ("a.mp3", io.BytesIO(b"AUDIO"), "audio/mpeg")}).status_code == 413
    assert staged == {}


def test_upload_reports_storage_outage(client, monkeypatch):
    def boom(*a, **k): raise storage.StorageUnavailable("minio down")
    monkeypatch.setattr(storage, "put_staging", boom)
    assert client.post("/uploads", files={"file": ("a.mp3", io.BytesIO(b"A"), "audio/mpeg")}).status_code == 503


def test_safe_name_strips_paths_and_odd_characters():
    assert uploads.safe_name("../../etc/passwd") == "passwd"
    assert uploads.safe_name("C:\\dir\\clip 1.mp4") == "clip_1.mp4"
    assert uploads.safe_name("") == "upload"


def test_invalid_run_requests_are_422_before_anything_happens(client, prefect):
    assert client.post("/runs", json={"source": "nope"}).status_code == 422
    assert client.post("/runs", json={"source": "web_crawl", "seed_urls": ["https://x"], "keywords": ["a"], "max_depth": 1}).status_code == 422
    assert prefect.created == []


# ---- runs (real Postgres) ---------------------------------------------------------------------------

@needs_db
def test_create_run_records_a_queued_row_and_submits_the_documented_payload(client, prefect, cleanup):
    r = client.post("/runs", json=direct_url(title="Typed title", dry_run=True))
    assert r.status_code == 202
    run_id = r.json()["run_id"]; cleanup.append(run_id)
    sent = prefect.created[0]
    assert sent == dict(run_id=run_id, source_name="direct_url", params_version=1, dry_run=True,
                        params={"url": "https://h/a.mp3", "content_type": "audio", "user_provided": {"title": "Typed title"}})
    detail = client.get(f"/runs/{run_id}").json()
    assert detail["status"] == "queued" and detail["mode"] == "single" and detail["dry_run"] is True
    assert detail["prefect_flow_run_id"] == r.json()["prefect_flow_run_id"] and detail["counts"] == {}


@needs_db
def test_prefect_outage_marks_the_run_failed_and_says_so(client, prefect, cleanup):
    prefect.fail = True
    r = client.post("/runs", json=direct_url())
    assert r.status_code == 502
    run_id = r.json()["detail"]["run_id"]; cleanup.append(run_id)
    detail = client.get(f"/runs/{run_id}").json()
    assert detail["status"] == "failed" and "Prefect" in detail["error_message"]


@needs_db
def test_local_file_requires_an_existing_staged_object(client, prefect, staged, cleanup):
    assert client.post("/runs", json={"source": "local_file", "staging_key": "u/none.mp3", "content_type": "audio"}).status_code == 422
    assert prefect.created == []
    key = client.post("/uploads", files={"file": ("a.mp3", io.BytesIO(b"A"), "audio/mpeg")}).json()["staging_key"]
    r = client.post("/runs", json={"source": "local_file", "staging_key": key, "content_type": "audio"})
    assert r.status_code == 202; cleanup.append(r.json()["run_id"])
    assert prefect.created[0]["params"]["staging_key"] == key


@needs_db
def test_web_crawl_is_an_archive_run(client, prefect, cleanup):
    r = client.post("/runs", json={"source": "web_crawl", "seed_urls": ["https://x.org"], "keywords": ["tariff"]})
    cleanup.append(r.json()["run_id"])
    assert client.get(f"/runs/{r.json()['run_id']}").json()["mode"] == "archive"


@needs_db
def test_detail_summarizes_the_log_and_log_endpoint_filters_and_pages(client, prefect, cleanup):
    import psycopg2
    run_id = client.post("/runs", json=direct_url()).json()["run_id"]; cleanup.append(run_id)
    conn = psycopg2.connect(DSN)
    with conn.cursor() as cur:
        for i, (status, reason) in enumerate([("ingested", None), ("ingested", None),
                                              ("prefilter_rejected", "out_of_date_range"),
                                              ("content_rejected", "no_keyword_match")]):
            cur.execute("""INSERT INTO bronze.discovery_log (run_id, source_name, source_url, canonical_url, status, reason_code)
                           VALUES (%s, 'direct_url', %s, %s, %s, %s)""", (run_id, f"https://h/{i}", f"https://h/{i}", status, reason))
    conn.commit(); conn.close()

    detail = client.get(f"/runs/{run_id}").json()
    assert detail["counts"] == {"ingested": 2, "prefilter_rejected": 1, "content_rejected": 1}
    assert detail["reasons"] == {"out_of_date_range": 1, "no_keyword_match": 1} and detail["documents_ingested"] == 2

    everything = client.get(f"/runs/{run_id}/log").json()
    assert everything["total"] == 4 and [i["source_url"] for i in everything["items"]] == [f"https://h/{n}" for n in range(4)]
    assert client.get(f"/runs/{run_id}/log?status=ingested").json()["total"] == 2
    assert client.get(f"/runs/{run_id}/log?reason_code=no_keyword_match").json()["items"][0]["status"] == "content_rejected"
    page = client.get(f"/runs/{run_id}/log?limit=2&offset=2").json()
    assert page["total"] == 4 and len(page["items"]) == 2
    assert client.get(f"/runs/{run_id}/log?status=bogus").status_code == 422


@needs_db
def test_reconciliation_fixes_a_run_stuck_running_after_a_crash(client, prefect, cleanup):
    run_id = client.post("/runs", json=direct_url()).json()["run_id"]; cleanup.append(run_id)
    import psycopg2
    conn = psycopg2.connect(DSN)
    with conn.cursor() as cur:
        cur.execute("UPDATE collection.runs SET status = 'running', started_at = now() WHERE run_id = %s", (run_id,))
    conn.commit(); conn.close()

    prefect.state = "RUNNING"
    assert client.get(f"/runs/{run_id}").json()["status"] == "running"
    prefect.state = "CRASHED"
    detail = client.get(f"/runs/{run_id}").json()
    assert detail["status"] == "failed" and "CRASHED" in detail["error_message"] and detail["finished_at"]


@needs_db
def test_reconciliation_never_overwrites_a_result_the_flow_recorded(client, prefect, cleanup):
    run_id = client.post("/runs", json=direct_url()).json()["run_id"]; cleanup.append(run_id)
    assert runs_repo.mark_terminal(run_id, runs_repo.RunStatus.COMPLETED, None)
    prefect.state = "CRASHED"
    assert client.get(f"/runs/{run_id}").json()["status"] == "completed"
    assert runs_repo.mark_terminal(run_id, runs_repo.RunStatus.FAILED, "late") is False


@needs_db
def test_cancel_stops_the_prefect_run_and_terminal_runs_cannot_be_cancelled(client, prefect, cleanup):
    created = client.post("/runs", json=direct_url()).json(); run_id = created["run_id"]; cleanup.append(run_id)
    r = client.post(f"/runs/{run_id}/cancel")
    assert r.status_code == 200 and r.json()["status"] == "cancelled"
    assert prefect.cancelled == [created["prefect_flow_run_id"]]
    assert client.post(f"/runs/{run_id}/cancel").status_code == 409


@needs_db
def test_unknown_run_is_404_and_list_returns_recent_runs(client, prefect, cleanup):
    missing = uuid.uuid4()
    assert client.get(f"/runs/{missing}").status_code == 404
    assert client.get(f"/runs/{missing}/log").status_code == 404
    assert client.post(f"/runs/{missing}/cancel").status_code == 404
    run_id = client.post("/runs", json=direct_url()).json()["run_id"]; cleanup.append(run_id)
    listed = client.get("/runs?limit=5").json()["runs"]
    assert listed[0]["run_id"] == run_id and listed[0]["source_name"] == "direct_url"
    assert client.get("/runs?status=queued").status_code == 200 and client.get("/runs?status=nope").status_code == 422


@needs_db
def test_end_to_end_backend_creates_flow_runs_backend_reads(client, prefect, staged, tmp_path, cleanup):
    """The whole contract: backend row -> flow (real run loop + Postgres repo) -> backend reads status, counts and log."""
    import psycopg2
    from prefect_flows.collection.deps import Deps
    from prefect_flows.collection.fetcher import PoliteFetcher
    from prefect_flows.collection.recorder import DiscoveryRecorder
    from prefect_flows.collection.repo import PostgresRepo
    from prefect_flows.collection.runner import run_collection
    from prefect_flows.collection.workarea import WorkArea
    from fakes import FakeStore
    from test_staging_user_provided import FakeMinio

    key = client.post("/uploads", files={"file": ("speech.mp3", io.BytesIO(os.urandom(64)), "audio/mpeg")}).json()["staging_key"]
    created = client.post("/runs", json={"source": "local_file", "staging_key": key, "content_type": "audio", "title": "E2E"}).json()
    run_id = created["run_id"]; cleanup.append(run_id)
    sent = prefect.created[0]

    repo = PostgresRepo(lambda: psycopg2.connect(DSN))
    minio = FakeMinio({key: staged[key]})
    store = FakeStore(); store.client = minio
    deps = Deps(work=WorkArea(run_id, root=tmp_path / "work"), fetcher=PoliteFetcher(respect_robots=False),
                recorder=DiscoveryRecorder(run_id, sink=repo), store=store, repo=repo)
    try:
        run_collection(sent["run_id"], sent["source_name"], sent["params"], deps, dry_run=sent["dry_run"],
                       params_version=sent["params_version"], prefect_flow_run_id=created["prefect_flow_run_id"])
        detail = client.get(f"/runs/{run_id}").json()
        assert detail["status"] == "completed" and detail["counts"] == {"ingested": 1} and detail["documents_ingested"] == 1
        row = client.get(f"/runs/{run_id}/log").json()["items"][0]
        assert row["status"] == "ingested" and row["source_url"] == f"upload://{key}" and row["doc_id"]
        assert minio.removed == [key]                                        # staged upload cleaned up
    finally:
        conn = psycopg2.connect(DSN)
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze.documents WHERE first_run_id = %s", (run_id,))
        conn.commit(); conn.close()
