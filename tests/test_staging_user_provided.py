"""Flow side of phase 2: uploads parked in MinIO staging, and researcher-typed metadata reaching Bronze."""
import pytest
from pydantic import ValidationError

from prefect_flows.collection.enums import RunStatus
from prefect_flows.collection.runner import run_collection
from prefect_flows.sources import registry

from fakes import FakeStore
from test_runner_registry import make_deps


class FakeMinio:
    def __init__(self, objects):
        self.objects, self.removed = dict(objects), []

    def fget_object(self, bucket, key, target):
        assert bucket == "staging"
        open(target, "wb").write(self.objects[key])

    def remove_object(self, bucket, key):
        self.removed.append(key)


def deps_with_staging(tmp_path, objects):
    deps = make_deps(tmp_path)
    deps.store.client = FakeMinio(objects)
    return deps


def test_local_file_needs_exactly_one_location():
    with pytest.raises(ValidationError):
        registry.validate_params("local_file", {"content_type": "audio"})
    with pytest.raises(ValidationError):
        registry.validate_params("local_file", {"content_type": "audio", "path": "/a.mp3", "staging_key": "u/a.mp3"})
    registry.validate_params("local_file", {"content_type": "audio", "staging_key": "u/a.mp3"})
    registry.validate_params("local_file", {"content_type": "audio", "path": "/a.mp3"})


def test_staged_upload_is_ingested_then_removed_from_staging(tmp_path):
    deps = deps_with_staging(tmp_path, {"u-1/speech.mp3": b"AUDIO"})
    summary = run_collection("run-1", "local_file", {"staging_key": "u-1/speech.mp3", "content_type": "audio"}, deps)
    assert summary == {"ingested": 1}
    doc = deps.repo.docs[0]
    assert doc["source_url"] == "upload://u-1/speech.mp3" and doc["file_name"] == "speech.mp3"
    assert deps.store.objects and deps.store.client.removed == ["u-1/speech.mp3"]
    assert not (tmp_path / "work" / "run-1").exists()


def test_missing_staged_object_fails_the_candidate_and_still_cleans_up(tmp_path):
    deps = deps_with_staging(tmp_path, {})
    summary = run_collection("run-1", "local_file", {"staging_key": "u-1/gone.mp3", "content_type": "audio"}, deps)
    assert summary == {"fetch_failed": 1}
    assert deps.repo.runs["run-1"]["status"] == RunStatus.COMPLETED_WITH_ERRORS


def test_user_provided_metadata_reaches_bronze(tmp_path):
    deps = deps_with_staging(tmp_path, {"u-1/a.mp3": b"AUDIO"})
    typed = {"title": "My title", "language": "fr"}
    run_collection("run-1", "local_file", {"staging_key": "u-1/a.mp3", "content_type": "audio", "user_provided": typed}, deps)
    assert deps.repo.docs[0]["raw_metadata"]["user_provided"] == typed


def test_no_user_provided_leaves_metadata_untouched(tmp_path):
    deps = deps_with_staging(tmp_path, {"u-1/a.mp3": b"AUDIO"})
    run_collection("run-1", "local_file", {"staging_key": "u-1/a.mp3", "content_type": "audio"}, deps)
    assert "user_provided" not in (deps.repo.docs[0].get("raw_metadata") or {})
