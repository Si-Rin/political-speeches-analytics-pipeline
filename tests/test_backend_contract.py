"""Backend <-> Prefect contract: the only coupling is JSON params, enum values and the database schema."""
import ast
from pathlib import Path

import pytest

from backend import prefect_params
from backend.schemas import common, sources
from prefect_flows.collection import enums as flow_enums
from prefect_flows.sources import registry

ROOT = Path(__file__).resolve().parent.parent

SAMPLES = [
    sources.LocalFileRequest(source="local_file", staging_key="u-1/a.mp3", content_type="audio", title="T", language="fr"),
    sources.DirectUrlRequest(source="direct_url", url="https://h/a.mp3", content_type="audio"),
    sources.YoutubeRequest(source="youtube", url="https://youtu.be/x", download_format="audio", publication_date="2017-01-20"),
    sources.YoutubeRequest(source="youtube", url="https://youtu.be/x", download_format="video"),
    sources.InternetArchiveRequest(source="internet_archive", url="https://archive.org/details/x", notes="n"),
    sources.WebCrawlRequest(source="web_crawl", seed_urls=["https://x.org"], keywords=["tariff"], max_depth=3, max_pages=10),
]


@pytest.mark.parametrize("req", SAMPLES, ids=lambda r: r.source)
def test_every_backend_payload_validates_against_the_flow_models(req):
    payload = prefect_params.to_prefect_params(req)
    registry.validate_params(payload.source_name.value, payload.params)          # extra="forbid" catches drift
    assert payload.mode.value == registry.get(payload.source_name.value).mode.value


def test_every_backend_source_is_covered_and_registered():
    assert {r.source for r in SAMPLES} == {s.value for s in common.SourceName}
    assert {s.value for s in common.SourceName} <= set(registry.names())
    assert set(sources.SOURCE_MODE) == set(common.SourceName)


@pytest.mark.parametrize("name", ["Mode", "ContentType", "RunStatus", "CandidateStatus", "ReasonCode", "MatchScope"])
def test_enum_values_match_the_flow(name):
    assert {m.value for m in getattr(common, name)} == {m.value for m in getattr(flow_enums, name)}


def test_youtube_download_format_maps_to_audio_only():
    audio = prefect_params.to_prefect_params(SAMPLES[2]).params
    video = prefect_params.to_prefect_params(SAMPLES[3]).params
    assert audio["audio_only"] is True and video["audio_only"] is False


def test_typed_metadata_becomes_user_provided_and_empty_metadata_is_omitted():
    params = prefect_params.to_prefect_params(SAMPLES[0]).params
    assert params["user_provided"] == {"title": "T", "language": "fr"}
    assert "user_provided" not in prefect_params.to_prefect_params(SAMPLES[1]).params


def test_backend_never_imports_prefect_flows_or_prefect():
    offenders = []
    for path in (ROOT / "backend").rglob("*.py"):
        for node in ast.walk(ast.parse(path.read_text(encoding="utf-8"))):
            names = [a.name for a in node.names] if isinstance(node, ast.Import) else \
                    [node.module or ""] if isinstance(node, ast.ImportFrom) else []
            if any(n == "prefect" or n.startswith(("prefect.", "prefect_flows")) for n in names):
                offenders.append(f"{path.relative_to(ROOT)}: {names}")
    assert not offenders, offenders


def test_backend_dockerfile_does_not_ship_the_flow_code():
    assert "prefect_flows" not in (ROOT / "backend/Dockerfile").read_text()


@pytest.mark.parametrize("bad", [
    dict(source="direct_url", url="ftp://h/a", content_type="audio"),
    dict(source="local_file", staging_key="../etc/passwd", content_type="audio"),
    dict(source="web_crawl", seed_urls=["https://x"], keywords=["a"], max_depth=1),
    dict(source="web_crawl", seed_urls=["https://x"], keywords=["  "]),
    dict(source="youtube", url="https://youtu.be/x", download_format="text"),
    dict(source="youtube", url="https://youtu.be/x", surprise=1),
])
def test_invalid_requests_are_rejected(bad):
    from pydantic import TypeAdapter, ValidationError
    with pytest.raises(ValidationError):
        TypeAdapter(sources.RunRequest).validate_python(bad)
