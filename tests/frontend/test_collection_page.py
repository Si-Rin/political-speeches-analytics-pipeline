"""
Headless page tests via streamlit.testing.v1.AppTest: the script really runs, so import errors, exceptions and
widget wiring are all caught. api_client is monkeypatched (module-level, before the page imports it) so no network
call happens. file_uploader can't be driven by AppTest, so the local-file path is exercised at the function level
in test_single_forms_local_file.py instead.
"""
from pathlib import Path

import pytest
from streamlit.testing.v1 import AppTest

PAGE = str(Path(__file__).resolve().parent.parent.parent / "frontend/pages/collection.py")


def submit(at, form: str, label: str = "Start collection"):
    return at.button(key=f"FormSubmitter:{form}-{label}")


@pytest.fixture
def api(monkeypatch):
    import api_client
    calls = {"create_run": [], "probe_source": [], "upload_file": []}

    def create_run(source, dry_run=False, **fields):
        calls["create_run"].append(dict(source=source, dry_run=dry_run, **fields))
        return {"run_id": "11111111-1111-1111-1111-111111111111", "status": "queued"}

    def probe_source(location, is_local, content_type):
        calls["probe_source"].append(dict(location=location, is_local=is_local, content_type=content_type))
        return {"ok": True, "detected_title": "A Title", "detected_duration_seconds": 120}

    monkeypatch.setattr(api_client, "create_run", create_run)
    monkeypatch.setattr(api_client, "probe_source", probe_source)
    return calls


# ---------------------------------------------------------------------------
# General
# ---------------------------------------------------------------------------
def test_page_renders_without_exceptions():
    at = AppTest.from_file(PAGE).run()
    assert not at.exception
    assert at.title[0].value == "Add a source"


def test_default_source_is_local_file():
    at = AppTest.from_file(PAGE).run()
    assert at.radio(key="collection_single_source").value == "Local file"


def test_switching_mode_and_source_never_raises():
    at = AppTest.from_file(PAGE).run()
    for mode in ("Archive collection", "Single document"):
        at.radio(key="collection_mode").set_value(mode).run()
        assert not at.exception
    for source in ("Direct URL", "YouTube", "Internet Archive", "Local file"):
        at.radio(key="collection_single_source").set_value(source).run()
        assert not at.exception


# ---------------------------------------------------------------------------
# Direct URL
# ---------------------------------------------------------------------------
def test_direct_url_empty_submit_warns(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_single_source").set_value("Direct URL").run()
    submit(at, "single_direct_url").click().run()
    assert not at.exception
    assert any("Enter a URL" in w.value for w in at.warning)
    assert api["create_run"] == []


def test_direct_url_full_flow_preview_then_submit(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_single_source").set_value("Direct URL").run()
    at.text_input(key="direct_url_url").input("https://example.com/speech.mp3").run()
    at.radio(key="direct_url_content_type").set_value("audio").run()
    at.text_input(key="direct_url_title").input("My Title").run()

    submit(at, "single_direct_url", "Preview").click().run()
    assert not at.exception
    assert api["probe_source"] == [dict(location="https://example.com/speech.mp3", is_local=False, content_type="audio")]
    assert api["create_run"] == []
    assert any(m.value == "A Title" for m in at.metric)

    submit(at, "single_direct_url").click().run()
    assert not at.exception
    assert api["create_run"] == [dict(source="direct_url", dry_run=False, url="https://example.com/speech.mp3",
                                      content_type="audio", title="My Title", publication_date=None,
                                      language=None, notes=None)]
    assert any("Collection started" in s.value for s in at.success)


def test_direct_url_warns_on_youtube_or_ia_link(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_single_source").set_value("Direct URL").run()
    at.text_input(key="direct_url_url").input("https://youtu.be/abc123").run()
    submit(at, "single_direct_url").click().run()
    assert any("YouTube tab" in w.value for w in at.warning)


# ---------------------------------------------------------------------------
# YouTube
# ---------------------------------------------------------------------------
def test_youtube_download_format_reaches_create_run(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_single_source").set_value("YouTube").run()
    at.text_input(key="youtube_url").input("https://www.youtube.com/watch?v=abc").run()
    at.radio(key="youtube_format").set_value("audio").run()
    submit(at, "single_youtube").click().run()
    assert not at.exception
    assert api["create_run"][0]["download_format"] == "audio"
    assert api["create_run"][0]["source"] == "youtube"


# ---------------------------------------------------------------------------
# Internet Archive
# ---------------------------------------------------------------------------
def test_internet_archive_has_no_content_type_field_and_submits(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_single_source").set_value("Internet Archive").run()
    assert not any(r.key == "ia_content_type" for r in at.radio)
    at.text_input(key="ia_url").input("https://archive.org/details/x").run()
    submit(at, "single_internet_archive").click().run()
    assert api["create_run"][0] == dict(source="internet_archive", dry_run=False, url="https://archive.org/details/x",
                                        title=None, publication_date=None, language=None, notes=None)


# ---------------------------------------------------------------------------
# Archive mode (web_crawl)
# ---------------------------------------------------------------------------
def test_web_crawl_requires_seed_and_keywords(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_mode").set_value("Archive collection").run()
    at.radio(key="collection_archive_source").set_value("Web crawl (Advanced)").run()
    submit(at, "archive_web_crawl", "Start crawl").click().run()
    assert any("seed URL" in w.value for w in at.warning)
    assert api["create_run"] == []


def test_web_crawl_full_submit(api):
    at = AppTest.from_file(PAGE).run()
    at.radio(key="collection_mode").set_value("Archive collection").run()
    at.radio(key="collection_archive_source").set_value("Web crawl (Advanced)").run()
    at.text_area(key="crawl_seed_urls").input("https://example.com/speeches").run()
    at.text_area(key="crawl_keywords").input("tariff\nrally").run()
    at.text_input(key="crawl_domains").input("example.com").run()
    at.number_input(key="crawl_max_depth").set_value(3).run()
    submit(at, "archive_web_crawl", "Start crawl").click().run()
    assert not at.exception
    assert api["create_run"] == [dict(source="web_crawl", dry_run=False, seed_urls=["https://example.com/speeches"],
                                      keywords=["tariff", "rally"], allowed_domains=["example.com"],
                                      max_depth=3, max_pages=50)]
    assert any("Crawl started" in s.value for s in at.success)
