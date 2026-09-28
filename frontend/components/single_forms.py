"""
Single-document forms: one function per source (local file, direct URL, YouTube, Internet Archive item).
Each renders its own st.form, an optional "Preview" (probe or dry_run) step, and a "Start collection" submit that
calls api_client.create_run(). Returns the created run dict on submit, otherwise None — the page decides what to
show. Every widget key is prefixed by the source name so switching sources never collides in session_state.
"""
from typing import Optional

import streamlit as st

import api_client
from components import metadata_fields


def _show_probe_result(result: dict) -> None:
    if not result.get("ok", True):
        for w in result.get("warnings", []):
            st.error(w)
        return
    cols = st.columns(3)
    cols[0].metric("Title", result.get("detected_title") or "—")
    cols[1].metric("File name", result.get("detected_file_name") or "—")
    cols[2].metric("MIME type", result.get("detected_mime_type") or "—")
    cols2 = st.columns(3)
    size = result.get("detected_size_bytes")
    cols2[0].metric("Size", f"{size / 1_000_000:.1f} MB" if size else "—")
    duration = result.get("detected_duration_seconds")
    cols2[1].metric("Duration", f"{duration / 60:.1f} min" if duration else "—")
    cols2[2].metric("Uploader", result.get("detected_uploader") or "—")
    for w in result.get("warnings", []):
        st.warning(w)


def _show_run_result(run: Optional[dict]) -> None:
    if not run:
        return
    if run.get("status") == "failed":
        return  # api_client already showed the Prefect-outage error
    st.success(f"Collection started (run `{run['run_id']}`). Check History shortly for the new document.")


# ---------------------------------------------------------------------------
# Local file
# ---------------------------------------------------------------------------
def render_local_file() -> Optional[dict]:
    st.caption("Uploads the file to the backend, then starts collecting it.")
    with st.form("single_local_file"):
        uploaded = st.file_uploader("File", type=None, key="local_file_uploader")
        content_type = st.radio("Content type", ["text", "video", "audio"], horizontal=True, key="local_file_content_type")
        meta = metadata_fields.render("local_file")
        submitted = st.form_submit_button("Start collection", type="primary")

    if not submitted:
        return None
    if uploaded is None:
        st.warning("Choose a file first.")
        return None

    with st.spinner("Uploading..."):
        upload = api_client.upload_file(uploaded.name, uploaded.getvalue(), uploaded.type)
    if not upload:
        return None

    with st.spinner("Starting collection..."):
        run = api_client.create_run("local_file", staging_key=upload["staging_key"], content_type=content_type, **meta)
    _show_run_result(run)
    return run


# ---------------------------------------------------------------------------
# Direct URL
# ---------------------------------------------------------------------------
def render_direct_url() -> Optional[dict]:
    st.caption("For a direct link to a media or text file. YouTube and Internet Archive links belong in their own tabs.")
    with st.form("single_direct_url"):
        url = st.text_input("URL", placeholder="https://example.com/speech.mp4", key="direct_url_url")
        content_type = st.radio("Content type", ["text", "video", "audio"], horizontal=True, key="direct_url_content_type")
        meta = metadata_fields.render("direct_url")
        col1, col2 = st.columns(2)
        preview_clicked = col1.form_submit_button("Preview")
        submit_clicked = col2.form_submit_button("Start collection", type="primary")

    if not (preview_clicked or submit_clicked):
        return None
    if not url.strip():
        st.warning("Enter a URL first.")
        return None
    if any(d in url for d in ("youtube.com", "youtu.be")):
        st.warning("This looks like a YouTube URL — use the YouTube tab instead.")
    elif "archive.org" in url:
        st.warning("This looks like an Internet Archive URL — use the Internet Archive tab instead.")

    if preview_clicked:
        with st.spinner("Checking the URL..."):
            result = api_client.probe_source(url.strip(), is_local=False, content_type=content_type)
        if result:
            _show_probe_result(result)
        return None

    with st.spinner("Starting collection..."):
        run = api_client.create_run("direct_url", url=url.strip(), content_type=content_type, **meta)
    _show_run_result(run)
    return run


# ---------------------------------------------------------------------------
# YouTube
# ---------------------------------------------------------------------------
def render_youtube() -> Optional[dict]:
    st.caption("A single video's URL. For a whole playlist or channel, archive mode's YouTube collection is coming soon.")
    with st.form("single_youtube"):
        url = st.text_input("Video URL", placeholder="https://www.youtube.com/watch?v=...", key="youtube_url")
        download_format = st.radio("Download format", ["video", "audio"], horizontal=True, key="youtube_format",
                                   help="Audio only is smaller and faster if you just need the transcript.")
        meta = metadata_fields.render("youtube")
        col1, col2 = st.columns(2)
        preview_clicked = col1.form_submit_button("Preview")
        submit_clicked = col2.form_submit_button("Start collection", type="primary")

    if not (preview_clicked or submit_clicked):
        return None
    if not url.strip():
        st.warning("Enter a video URL first.")
        return None

    if preview_clicked:
        with st.spinner("Reading video metadata..."):
            result = api_client.probe_source(url.strip(), is_local=False, content_type="video")
        if result:
            _show_probe_result(result)
        return None

    with st.spinner("Starting collection..."):
        run = api_client.create_run("youtube", url=url.strip(), download_format=download_format, **meta)
    _show_run_result(run)
    return run


# ---------------------------------------------------------------------------
# Internet Archive (item)
# ---------------------------------------------------------------------------
def render_internet_archive() -> Optional[dict]:
    st.caption("One archive.org item URL (e.g. a TV News clip). Always collected as audio.")
    with st.form("single_internet_archive"):
        url = st.text_input("Item URL", placeholder="https://archive.org/details/...", key="ia_url")
        meta = metadata_fields.render("internet_archive")
        col1, col2 = st.columns(2)
        preview_clicked = col1.form_submit_button("Preview")
        submit_clicked = col2.form_submit_button("Start collection", type="primary")

    if not (preview_clicked or submit_clicked):
        return None
    if not url.strip():
        st.warning("Enter an item URL first.")
        return None

    if preview_clicked:
        with st.spinner("Checking the item..."):
            result = api_client.probe_source(url.strip(), is_local=False, content_type="audio")
        if result:
            _show_probe_result(result)
        return None

    with st.spinner("Starting collection..."):
        run = api_client.create_run("internet_archive", url=url.strip(), **meta)
    _show_run_result(run)
    return run


RENDERERS = {
    "Local file": render_local_file,
    "Direct URL": render_direct_url,
    "YouTube": render_youtube,
    "Internet Archive": render_internet_archive,
}
