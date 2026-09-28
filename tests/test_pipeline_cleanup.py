"""Every outcome of consuming a candidate must leave the work area empty and never touch a user's own files."""
import pytest

from prefect_flows.collection.enums import CandidateStatus as S, ReasonCode
from prefect_flows.collection.fetcher import FetchError
from prefect_flows.collection.handles import ContentHandle, HttpHandle, LocalPathHandle, TempFileHandle
from prefect_flows.collection.pipeline import ingest_candidate
from prefect_flows.collection.recorder import DiscoveryRecorder
from prefect_flows.collection.workarea import WorkArea
from prefect_flows.sources.base import Candidate

from fakes import FakeRepo, FakeStore


class StubFetcher:
    """Writes the URL's fake content into the scope, like PoliteFetcher.download."""
    def __init__(self, body=b"downloaded", error=None):
        self.body, self.error = body, error
    def download(self, url, scope, filename=None, check_robots=False):
        if self.error:
            raise self.error
        path = scope.path_for(filename or "f.bin")
        path.write_bytes(self.body)
        return path


class ExplodingHandle(ContentHandle):
    def materialize(self, ctx):
        raise RuntimeError("disk full")


def consume(tmp_path, candidate, *, store=None, repo=None, fetcher=None, dry_run=False):
    store, repo = store or FakeStore(), repo or FakeRepo()
    recorder = DiscoveryRecorder("run-1")
    with WorkArea("run-1", root=tmp_path / "work") as work:
        status = ingest_candidate(candidate, source_name="direct_url", run_id="run-1", work=work,
                                  fetcher=fetcher or StubFetcher(), store=store, bronze=repo,
                                  recorder=recorder, dry_run=dry_run)
        leftovers = [p for p in work.run_dir.rglob("*") if p.is_file()]
        assert leftovers == [], f"files left in work area: {leftovers}"
    return status, recorder, store, repo


def cand(handle, name="a.mp3", **kw):
    return Candidate(source_url="https://h/a.mp3", source_type="audio", file_name=name, handle=handle, **kw)


def test_ingested_http(tmp_path):
    status, rec, store, repo = consume(tmp_path, cand(HttpHandle("https://h/a.mp3", filename="a.mp3")))
    assert status == S.INGESTED and len(store.objects) == 1 and len(repo.docs) == 1
    assert rec.entries[0].doc_id == 1
    meta = repo.docs[0]["raw_metadata"]
    assert meta["source_name"] == "direct_url" and meta["collection"]["run_id"] == "run-1"


def test_local_file_is_never_deleted(tmp_path):
    f = tmp_path / "mine.mp3"; f.write_bytes(b"user data")
    status, *_ = consume(tmp_path, cand(LocalPathHandle(f)))
    assert status == S.INGESTED and f.read_bytes() == b"user data"


def test_temp_handle_scope_is_released_on_every_path(tmp_path):
    for outcome in ("ingested", "duplicate", "upload_failed", "db_error"):
        work_root = tmp_path / outcome
        with WorkArea("seed", root=work_root) as seed:
            scope = seed.scope()
            path = scope.path_for("yt.mp4"); path.write_bytes(b"video-bytes-" + outcome.encode())
            repo = FakeRepo(insert_error=outcome == "db_error")
            store = FakeStore(fail_on=(lambda k: True) if outcome == "upload_failed" else None)
            if outcome == "duplicate":
                import hashlib
                repo.checksums.add(hashlib.sha256(path.read_bytes()).hexdigest())
            recorder = DiscoveryRecorder("r")
            c = cand(TempFileHandle(path, scope), name="yt.mp4")
            ingest_candidate(c, source_name="youtube", run_id="r", work=seed, fetcher=None, store=store,
                             bronze=repo, recorder=recorder)
            assert scope.released and not path.exists(), outcome


def test_duplicate_content(tmp_path):
    repo = FakeRepo()
    consume(tmp_path, cand(HttpHandle("https://h/a.mp3", filename="a.mp3")), repo=repo)
    status, rec, store, _ = consume(tmp_path, cand(HttpHandle("https://h/a.mp3", filename="a.mp3")), repo=repo)
    assert status == S.DUPLICATE_CONTENT and rec.entries[0].reason == ReasonCode.CHECKSUM_EXISTS and store.objects == {}


def test_lost_race_counts_as_duplicate(tmp_path):
    status, rec, *_ = consume(tmp_path, cand(HttpHandle("https://h/a.mp3", filename="a.mp3")), repo=FakeRepo(race=True))
    assert status == S.DUPLICATE_CONTENT


@pytest.mark.parametrize("error, reason", [
    (FetchError(ReasonCode.TIMEOUT, "slow"), ReasonCode.TIMEOUT),
    (FetchError(ReasonCode.CONTENT_TOO_LARGE, "big"), ReasonCode.CONTENT_TOO_LARGE),
])
def test_fetch_failed_keeps_reason(tmp_path, error, reason):
    status, rec, store, repo = consume(tmp_path, cand(HttpHandle("https://h/a.mp3")), fetcher=StubFetcher(error=error))
    assert status == S.FETCH_FAILED and rec.entries[0].reason == reason and not store.objects and not repo.docs


def test_unexpected_materialize_error_is_fetch_failed(tmp_path):
    status, rec, *_ = consume(tmp_path, cand(ExplodingHandle()))
    assert status == S.FETCH_FAILED and "disk full" in rec.entries[0].detail


def test_missing_local_file_is_fetch_failed(tmp_path):
    status, *_ = consume(tmp_path, cand(LocalPathHandle(tmp_path / "nope.mp3")))
    assert status == S.FETCH_FAILED


def test_upload_and_db_failures_are_ingest_failed(tmp_path):
    status, rec, *_ = consume(tmp_path, cand(HttpHandle("https://h/a", filename="a.mp3")),
                              store=FakeStore(fail_on=lambda k: True))
    assert status == S.INGEST_FAILED and rec.entries[0].reason == ReasonCode.UPLOAD_FAILED
    status, rec, *_ = consume(tmp_path, cand(HttpHandle("https://h/a", filename="a.mp3")), repo=FakeRepo(insert_error=True))
    assert status == S.INGEST_FAILED and rec.entries[0].reason == ReasonCode.DB_ERROR


def test_dry_run_records_and_downloads_nothing(tmp_path):
    fetcher = StubFetcher(error=AssertionError("must not download in a dry run"))
    status, rec, store, repo = consume(tmp_path, cand(HttpHandle("https://h/a.mp3")), fetcher=fetcher, dry_run=True)
    assert status == S.PREFILTER_PASSED and not store.objects and not repo.docs


def test_staging_handle_deletes_object_after_use(tmp_path):
    from prefect_flows.collection.handles import StagingObjectHandle

    class Minio:
        def __init__(self): self.removed = []
        def fget_object(self, bucket, key, target): open(target, "wb").write(b"upload")
        def remove_object(self, bucket, key): self.removed.append((bucket, key))
    client = Minio()
    status, *_ = consume(tmp_path, cand(StagingObjectHandle(client, "u-1.mp3")))
    assert status == S.INGESTED and client.removed == [("staging", "u-1.mp3")]
