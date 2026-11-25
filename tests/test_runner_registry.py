import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from prefect_flows.collection.deps import Deps
from prefect_flows.collection.enums import RunStatus, SourceName
from prefect_flows.collection.fetcher import PoliteFetcher
from prefect_flows.collection.recorder import DiscoveryRecorder
from prefect_flows.collection.runner import run_collection
from prefect_flows.collection.workarea import WorkArea
from prefect_flows.sources import registry

from fakes import FakeRepo, FakeStore


def make_deps(tmp_path, repo=None, store=None, run_id="run-1"):
    repo = repo or FakeRepo()
    return Deps(work=WorkArea(run_id, root=tmp_path / "work"), fetcher=PoliteFetcher(respect_robots=False),
                recorder=DiscoveryRecorder(run_id), store=store or FakeStore(), repo=repo)


def test_every_source_name_is_registered_except_future_ones():
    future = {SourceName.YOUTUBE_COLLECTION, SourceName.INTERNET_ARCHIVE_COLLECTION}   # phases 4c/4d
    assert {SourceName(n) for n in registry.names()} == set(SourceName) - future


def test_unknown_source_and_bad_params():
    with pytest.raises(ValueError):
        registry.get("nope")
    with pytest.raises(ValidationError):
        registry.validate_params("web_crawl", {"seed_urls": ["https://x"], "keywords": ["a"], "max_depth": 1})
    with pytest.raises(ValidationError):
        registry.validate_params("direct_url", {"url": "https://x/a.mp3", "content_type": "audio", "surprise": 1})
    with pytest.raises(ValidationError):
        registry.validate_params("miller_center", {})            # needs urls or start_url


def test_flow_module_has_no_source_specific_code():
    text = (Path(__file__).resolve().parent.parent / "prefect_flows/bronze_ingest.py").read_text()
    code = re.sub(r'""".*?"""', "", text, flags=re.S)
    for word in ("youtube", "ucsb", "miller", "archive.org", "internet_archive", "crawl", "yt_dlp"):
        assert word not in code.lower(), f"source-specific word {word!r} in the flow"


def test_full_run_completed_and_streamed(tmp_path):
    folder = tmp_path / "in"; folder.mkdir()
    (folder / "a.mp3").write_bytes(b"AAA"); (folder / "b.mp3").write_bytes(b"BBB")
    (folder / "b_copy.mp3").write_bytes(b"BBB"); (folder / "notes.xyz").write_bytes(b"ignored")
    deps = make_deps(tmp_path)
    summary = run_collection("run-1", "local_folder", {"folder": str(folder)}, deps)
    assert summary == {"ingested": 2, "duplicate_content": 1}
    assert deps.repo.runs["run-1"]["status"] == RunStatus.COMPLETED
    assert all(p.exists() for p in folder.iterdir())                              # user files untouched
    assert not (tmp_path / "work" / "run-1").exists()


def test_run_completed_with_errors(tmp_path):
    folder = tmp_path / "in"; folder.mkdir(); (folder / "a.mp3").write_bytes(b"AAA")
    deps = make_deps(tmp_path, store=FakeStore(fail_on=lambda k: True))
    summary = run_collection("run-1", "local_folder", {"folder": str(folder)}, deps)
    assert summary == {"ingest_failed": 1} and deps.repo.runs["run-1"]["status"] == RunStatus.COMPLETED_WITH_ERRORS


def test_run_failed_on_source_error_and_on_bad_params(tmp_path):
    deps = make_deps(tmp_path)
    with pytest.raises(FileNotFoundError):
        run_collection("run-1", "local_folder", {"folder": str(tmp_path / "missing")}, deps)
    assert deps.repo.runs["run-1"]["status"] == RunStatus.FAILED
    deps2 = make_deps(tmp_path, run_id="run-2")
    with pytest.raises(ValidationError):
        run_collection("run-2", "local_folder", {"nope": 1}, deps2)
    assert deps2.repo.runs["run-2"]["status"] == RunStatus.FAILED


def test_dry_run_ingests_nothing(tmp_path):
    folder = tmp_path / "in"; folder.mkdir(); (folder / "a.mp3").write_bytes(b"AAA")
    deps = make_deps(tmp_path)
    assert run_collection("run-1", "local_folder", {"folder": str(folder)}, deps, dry_run=True) == {"prefilter_passed": 1}
    assert deps.store.objects == {} and deps.repo.docs == []
