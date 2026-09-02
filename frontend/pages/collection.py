"""
Collection page: add a source -> review detected information -> submit -> see confirmation.

Actual ingestion progress is tracked on the Pipeline Status page (this page doesn't block/wait for ingestion to finish).
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # so `import api_client` resolves

import streamlit as st

from api_client import probe_source, submit_source

st.set_page_config(layout="wide")

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
        # Only display metadata that was actually detected.
        # This avoids showing placeholder dashes for fields that do not
        # apply to a particular source type (e.g. YouTube URLs).
        detected_fields = []

        title = result.get("detected_title")
        if title:
            detected_fields.append(("Title", title))

        file_name = result.get("detected_file_name")
        if file_name:
            detected_fields.append(("File name", file_name))

        mime_type = result.get("detected_mime_type")
        if mime_type:
            detected_fields.append(("MIME type", mime_type))

        size = result.get("detected_size_bytes")
        if size:
            detected_fields.append(("Size", f"{size / 1_000_000:.1f} MB"))

        duration = result.get("detected_duration_seconds")
        if duration:
            detected_fields.append(("Duration", f"{duration / 60:.1f} min"))

        if detected_fields:
            columns = st.columns(min(3, len(detected_fields)))
            for index, (label, value) in enumerate(detected_fields):
                columns[index % len(columns)].metric(label, value)
        else:
            st.info("No metadata could be detected automatically.")

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
