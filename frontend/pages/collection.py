"""
Collection page: add a source -> review detected information -> submit -> see confirmation.

Actual ingestion progress is tracked on the Pipeline Status page (this page doesn't block/wait for ingestion to finish).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # so `import api_client` resolves

import streamlit as st

from api_client import probe_source, submit_source

st.title("Add a source")

if "probe_result" not in st.session_state:
    st.session_state.probe_result = None
if "probed_for" not in st.session_state:
    st.session_state.probed_for = None

with st.form("add_source_form"):
    source_kind = st.radio("Source", ["URL", "Local path"], horizontal=True)
    location = st.text_input(
        "Location",
        placeholder="https://..." if source_kind == "URL" else "/path/to/file.mp4",
    )
    content_type = st.radio("Content type", ["text", "video", "audio"], horizontal=True)
    detect_clicked = st.form_submit_button("Detect")

if detect_clicked:
    if not location.strip():
        st.warning("Enter a URL or local path first.")
    else:
        is_local = source_kind == "Local path"
        with st.spinner("Detecting..."):
            result = probe_source(location.strip(), is_local, content_type)
        st.session_state.probe_result = result
        st.session_state.probed_for = {
            "location": location.strip(), "is_local": is_local, "content_type": content_type,
        }

result = st.session_state.probe_result
probed_for = st.session_state.probed_for

if result is not None:
    st.subheader("Detected information")
    if not result.get("ok"):
        for w in result.get("warnings", []):
            st.error(w)
    else:
        cols = st.columns(3)
        cols[0].metric("Title", result.get("detected_title") or "—")
        cols[1].metric("File name", result.get("detected_file_name") or "—")
        cols[2].metric("MIME type", result.get("detected_mime_type") or "—")

        cols2 = st.columns(2)
        size = result.get("detected_size_bytes")
        cols2[0].metric("Size", f"{size / 1_000_000:.1f} MB" if size else "—")
        duration = result.get("detected_duration_seconds")
        cols2[1].metric("Duration", f"{duration / 60:.1f} min" if duration else "—")

        for w in result.get("warnings", []):
            st.warning(w)

        if st.button("Confirm & submit", type="primary"):
            with st.spinner("Starting ingestion..."):
                submit_result = submit_source(
                    probed_for["location"],
                    probed_for["is_local"],
                    probed_for["content_type"],
                    result.get("raw_metadata"),
                )
            if submit_result and submit_result.get("accepted"):
                st.success(submit_result["message"])
                st.session_state.probe_result = None
                st.session_state.probed_for = None
            else:
                st.error("Submission failed.")