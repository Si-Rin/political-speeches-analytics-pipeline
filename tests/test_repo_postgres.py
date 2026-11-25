"""
Integration test against a real Postgres that already has init_schema.sql + updates.sql applied.
Skipped unless TEST_DATABASE_URL is set, e.g.
    TEST_DATABASE_URL="host=127.0.0.1 port=5433 dbname=speeches user=app password=app" pytest tests/test_repo_postgres.py
"""
import os
import uuid

import pytest

psycopg2 = pytest.importorskip("psycopg2")
DSN = os.environ.get("TEST_DATABASE_URL")
pytestmark = pytest.mark.skipif(not DSN, reason="TEST_DATABASE_URL not set")

from prefect_flows.collection.deps import Deps
from prefect_flows.collection.enums import CandidateStatus, MatchScope, Mode, ReasonCode, RunStatus
from prefect_flows.collection.fetcher import PoliteFetcher
from prefect_flows.collection.recorder import DiscoveryEntry, DiscoveryRecorder
from prefect_flows.collection.repo import PostgresRepo
from prefect_flows.collection.runner import run_collection
from prefect_flows.collection.workarea import WorkArea

from fakes import FakeStore


@pytest.fixture
def repo():
    return PostgresRepo(lambda: psycopg2.connect(DSN))


def q(sql, *params):
    conn = psycopg2.connect(DSN)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


@pytest.fixture
def run_id(repo):
    rid = str(uuid.uuid4())
    yield rid
    q("SELECT 1")   # keep the connection helper exercised
    conn = psycopg2.connect(DSN)
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM bronze.documents WHERE first_run_id = %s", (rid,))
        cur.execute("DELETE FROM collection.runs WHERE run_id = %s", (rid,))
    conn.close()


def test_run_lifecycle_and_discovery_log(repo, run_id):
    repo.ensure_run(run_id, Mode.ARCHIVE, "ucsb", {"a": 1}, 1, False)
    repo.ensure_run(run_id, Mode.ARCHIVE, "ucsb", {"a": 1}, 1, False)       # idempotent
    assert repo.get_run_status(run_id) == "queued"
    repo.mark_running(run_id)
    repo.write_discovery(run_id, DiscoveryEntry(
        source_name="ucsb", source_url="https://x/a", canonical_url="https://x/a", status=CandidateStatus.CONTENT_REJECTED,
        reason=ReasonCode.NO_KEYWORD_MATCH, matched_keywords=[], match_scope=MatchScope.ABOUT, keywords_hash="abc"))
    repo.finalize_run(run_id, RunStatus.COMPLETED, None, 3)
    assert q("SELECT status, listing_pages_visited, started_at IS NOT NULL, finished_at IS NOT NULL FROM collection.runs WHERE run_id=%s", run_id) == [("completed", 3, True, True)]
    assert q("SELECT status, reason_code, match_scope FROM bronze.discovery_log WHERE run_id=%s", run_id) == [("content_rejected", "no_keyword_match", "about")]


def test_insert_document_conflict_and_known_url(repo, run_id):
    repo.ensure_run(run_id, Mode.SINGLE, "direct_url", {}, 1, False)
    checksum = uuid.uuid4().hex
    kw = dict(source_url="https://example.org/s/1", source_type="text", file_name="1.html", file_size=3,
              mime_type="text/html", checksum=checksum, raw_metadata={"k": "v"}, storage_path=checksum + ".html", run_id=run_id)
    assert isinstance(repo.insert_document(**kw), int)
    assert repo.insert_document(**kw) is None                                # same checksum: no exception, no row
    assert repo.checksum_exists(checksum)
    assert repo.known_url("https://example.org/s/1", "text")
    assert repo.known_url("https://example.org/s/1")
    assert not repo.known_url("https://example.org/s/1", "audio")            # different format is not "known"
    assert not repo.known_url("https://example.org/other")


def test_full_run_writes_everything(repo, run_id, tmp_path):
    folder = tmp_path / "in"; folder.mkdir()
    (folder / f"{uuid.uuid4().hex}.mp3").write_bytes(uuid.uuid4().bytes)
    deps = Deps(work=WorkArea(run_id, root=tmp_path / "work"), fetcher=PoliteFetcher(respect_robots=False),
                recorder=DiscoveryRecorder(run_id, sink=repo), store=FakeStore(), repo=repo)
    assert run_collection(run_id, "local_folder", {"folder": str(folder)}, deps) == {"ingested": 1}
    assert repo.get_run_status(run_id) == "completed"
    assert q("SELECT status, doc_id IS NOT NULL FROM bronze.discovery_log WHERE run_id=%s", run_id) == [("ingested", True)]
    ((raw, first_run),) = q("SELECT raw_metadata, first_run_id::text FROM bronze.documents WHERE first_run_id=%s", run_id)
    assert first_run == run_id and raw["collection"]["run_id"] == run_id and raw["source_name"] == "local_folder"
