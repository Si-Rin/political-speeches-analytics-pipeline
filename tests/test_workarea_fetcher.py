import os
import time

import pytest
import requests

from prefect_flows.collection.budget import CollectionBudget
from prefect_flows.collection.enums import ReasonCode
from prefect_flows.collection.fetcher import FetchError, PoliteFetcher
from prefect_flows.collection.workarea import WorkArea, WorkAreaFull


def test_workarea_scopes_and_cleanup(tmp_path):
    with WorkArea("run1", root=tmp_path) as work:
        s1, s2 = work.scope(), work.scope()
        (s1.path_for("../../escape.txt")).write_text("x")
        assert s1.path_for("../../escape.txt").parent == s1.path      # cannot escape the scope
        s1.release(); s1.release()                                     # idempotent
        assert not s1.path.exists() and s2.path.exists()
    assert not (tmp_path / "run1").exists()                            # run dir removed on exit


def test_workarea_removed_on_exception(tmp_path):
    with pytest.raises(RuntimeError):
        with WorkArea("run1", root=tmp_path) as work:
            work.scope().path_for("a").write_text("x")
            raise RuntimeError("boom")
    assert not (tmp_path / "run1").exists()


def test_workarea_sweeps_stale_dirs_only(tmp_path):
    old, fresh = tmp_path / "old-run", tmp_path / "fresh-run"
    old.mkdir(); fresh.mkdir()
    ancient = time.time() - 48 * 3600
    os.utime(old, (ancient, ancient))
    with WorkArea("run2", root=tmp_path, stale_hours=24):
        assert not old.exists() and fresh.exists()


def test_workarea_max_bytes(tmp_path):
    with WorkArea("r", root=tmp_path, max_bytes=10) as work:
        work.scope().path_for("big").write_bytes(b"x" * 11)
        with pytest.raises(WorkAreaFull):
            work.check_budget()


def test_budget():
    b = CollectionBudget(max_listing_pages=2, hard_max_documents=1)
    assert b.take_listing_page() and b.take_listing_page() and not b.take_listing_page()
    assert b.take_document() and not b.take_document() and b.exhausted


class FakeResp:
    def __init__(self, status=200, body=b"data", headers=None, text=None):
        self.status_code, self.body, self.headers = status, body, headers or {}
        self.text = text if text is not None else body.decode("utf-8", "ignore")
        self.encoding = "utf-8"; self.apparent_encoding = "utf-8"
    def iter_content(self, chunk_size=1):
        for i in range(0, len(self.body), 4):
            yield self.body[i:i + 4]
    def close(self): pass
    def __enter__(self): return self
    def __exit__(self, *a): self.close()


class FakeSession:
    def __init__(self, routes):
        self.routes, self.headers, self.calls = routes, {}, []
    def get(self, url, stream=False, timeout=None):
        self.calls.append(url)
        r = self.routes.get(url, FakeResp(404))
        if isinstance(r, Exception):
            raise r
        if isinstance(r, list):
            r = r.pop(0) if len(r) > 1 else r[0]
        return r


def fetcher(routes, **kw):
    sleeps = []
    f = PoliteFetcher(session=FakeSession(routes), sleep=sleeps.append, clock=lambda: 0.0, **kw)
    f.sleeps = sleeps
    return f


def test_download_streams_into_scope(tmp_path):
    f = fetcher({"https://h/a.mp3": FakeResp(body=b"0123456789")}, respect_robots=False)
    with WorkArea("r", root=tmp_path) as work, work.scope() as scope:
        path = f.download("https://h/a.mp3", scope)
        assert path.read_bytes() == b"0123456789" and path.parent == scope.path


def test_retries_then_succeeds_and_gives_up(tmp_path):
    f = fetcher({"https://h/x": [FakeResp(503), FakeResp(200, b"ok")]}, respect_robots=False, backoff_seconds=3)
    assert f.get_text("https://h/x") == "ok" and 3 in f.sleeps
    g = fetcher({"https://h/x": FakeResp(503)}, respect_robots=False, max_retries=1)
    with pytest.raises(FetchError) as e:
        g.get_text("https://h/x")
    assert e.value.reason == ReasonCode.HTTP_ERROR


@pytest.mark.parametrize("resp, reason", [
    (FakeResp(404), ReasonCode.HTTP_ERROR), (FakeResp(403), ReasonCode.ACCESS_RESTRICTED),
    (requests.Timeout(), ReasonCode.TIMEOUT),
])
def test_error_mapping(resp, reason):
    f = fetcher({"https://h/x": resp}, respect_robots=False, max_retries=0)
    with pytest.raises(FetchError) as e:
        f.get_text("https://h/x")
    assert e.value.reason == reason


def test_size_cap_by_header_and_by_stream(tmp_path):
    f = fetcher({"https://h/a": FakeResp(body=b"x" * 50, headers={"Content-Length": "50"}),
                 "https://h/b": FakeResp(body=b"x" * 50)}, respect_robots=False, max_bytes=10)
    with WorkArea("r", root=tmp_path) as work, work.scope() as scope:
        for u in ("https://h/a", "https://h/b"):
            with pytest.raises(FetchError) as e:
                f.download(u, scope)
            assert e.value.reason == ReasonCode.CONTENT_TOO_LARGE
        assert list(scope.path.iterdir()) == []                   # partial file removed


def test_robots_disallow_delay_and_unreachable_robots(tmp_path):
    robots = FakeResp(200, b"User-agent: *\nDisallow: /private/\n")
    f = fetcher({"https://h/robots.txt": robots, "https://h/ok": FakeResp(body=b"a"), "https://h/private/x": FakeResp()},
                delay_seconds=5)
    assert f.get_text("https://h/ok") == "a"
    with pytest.raises(FetchError) as e:
        f.get_text("https://h/private/x")
    assert e.value.reason == ReasonCode.ROBOTS_DISALLOWED
    f.get_text("https://h/ok")
    assert any(s > 0 for s in f.sleeps)                            # same host, same clock: it had to wait
    down = fetcher({"https://h/robots.txt": requests.ConnectionError(), "https://h/ok": FakeResp(body=b"a")})
    assert down.get_text("https://h/ok") == "a"                    # unreachable robots.txt must not block the run
