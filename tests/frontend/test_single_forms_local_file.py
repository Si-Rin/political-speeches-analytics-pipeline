"""
render_local_file() can't be driven through AppTest (file_uploader has no test API), so it's exercised directly:
st.* calls are captured via a lightweight fake in place of the streamlit module, and the widgets it reads
(file_uploader/radio/form_submit_button) are monkeypatched to fixed return values.
"""
import types

import pytest

import api_client
from components import single_forms


class FakeUpload:
    def __init__(self, name, data, mime):
        self.name, self._data, self.type = name, data, mime

    def getvalue(self):
        return self._data


@pytest.fixture
def calls(monkeypatch):
    log = {"upload_file": [], "create_run": []}

    def upload_file(name, data, content_type):
        log["upload_file"].append((name, data, content_type))
        return {"staging_key": "u-1/a.mp3", "file_name": name, "size_bytes": len(data)}

    def create_run(source, dry_run=False, **fields):
        log["create_run"].append(dict(source=source, dry_run=dry_run, **fields))
        return {"run_id": "r1", "status": "queued"}

    monkeypatch.setattr(api_client, "upload_file", upload_file)
    monkeypatch.setattr(api_client, "create_run", create_run)
    return log


class _NullContext:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _stub_streamlit(monkeypatch, *, uploaded, content_type, submitted, meta):
    """Patch just enough of st.* for render_local_file() to run headless."""
    import streamlit as st

    monkeypatch.setattr(st, "form", lambda *a, **k: _NullContext())
    monkeypatch.setattr(st, "caption", lambda *a, **k: None)
    monkeypatch.setattr(st, "file_uploader", lambda *a, **k: uploaded)
    monkeypatch.setattr(st, "radio", lambda *a, **k: content_type)
    monkeypatch.setattr(st, "form_submit_button", lambda *a, **k: submitted)
    monkeypatch.setattr(single_forms.metadata_fields, "render", lambda prefix: meta)
    monkeypatch.setattr(st, "spinner", lambda *a, **k: _NullContext())
    warnings = []
    monkeypatch.setattr(st, "warning", lambda msg: warnings.append(msg))
    return warnings


META = {"title": None, "publication_date": None, "language": None, "notes": None}


def test_no_file_chosen_warns_and_does_not_call_the_api(monkeypatch, calls):
    warnings = _stub_streamlit(monkeypatch, uploaded=None, content_type="audio", submitted=True, meta=META)
    assert single_forms.render_local_file() is None
    assert warnings == ["Choose a file first."]
    assert calls == {"upload_file": [], "create_run": []}


def test_not_submitted_does_nothing(monkeypatch, calls):
    _stub_streamlit(monkeypatch, uploaded=FakeUpload("a.mp3", b"AAA", "audio/mpeg"), content_type="audio",
                    submitted=False, meta=META)
    assert single_forms.render_local_file() is None
    assert calls == {"upload_file": [], "create_run": []}


def test_upload_then_create_run_with_the_staging_key(monkeypatch, calls):
    upload = FakeUpload("My Speech.mp3", b"AUDIOBYTES", "audio/mpeg")
    meta = {"title": "T", "publication_date": None, "language": "fr", "notes": None}
    _stub_streamlit(monkeypatch, uploaded=upload, content_type="audio", submitted=True, meta=meta)

    result = single_forms.render_local_file()

    assert calls["upload_file"] == [("My Speech.mp3", b"AUDIOBYTES", "audio/mpeg")]
    assert calls["create_run"] == [dict(source="local_file", dry_run=False, staging_key="u-1/a.mp3",
                                        content_type="audio", **meta)]
    assert result == {"run_id": "r1", "status": "queued"}


def test_upload_failure_never_calls_create_run(monkeypatch, calls):
    upload = FakeUpload("a.mp3", b"A", "audio/mpeg")
    _stub_streamlit(monkeypatch, uploaded=upload, content_type="audio", submitted=True, meta=META)
    monkeypatch.setattr(api_client, "upload_file", lambda *a, **k: None)   # backend/storage failure
    assert single_forms.render_local_file() is None
    assert calls["create_run"] == []
