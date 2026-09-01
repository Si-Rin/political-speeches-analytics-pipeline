"""User-friendly Streamlit interface for collecting political-speech data."""

import re
import tempfile
from pathlib import Path
from urllib.parse import urlparse

import streamlit as st

from prefect_flows.collection_ingest import collect_document


st.set_page_config(
    page_title="Political Speeches · Collect",
    page_icon="🎙️",
    layout="centered",
)


SUPPORTED_UPLOADS = [
    "mp4", "mov", "mkv", "avi",
    "mp3", "wav", "m4a", "flac", "webm",
    "txt", "html", "htm", "pdf",
]

TYPE_LABELS = {
    "text": "📄 Text",
    "audio": "🎧 Audio",
    "video": "🎥 Video",
}


def is_valid_url(value: str) -> bool:
    try:
        parsed = urlparse(value.strip())
        return parsed.scheme in {"http", "https"} and bool(parsed.netloc)
    except ValueError:
        return False


def detect_type_from_filename(name: str) -> str | None:
    suffix = Path(name).suffix.lower()
    if suffix in {".mp4", ".mov", ".mkv", ".avi"}:
        return "video"
    if suffix in {".mp3", ".wav", ".m4a", ".flac", ".webm"}:
        return "audio"
    if suffix in {".txt", ".html", ".htm", ".pdf"}:
        return "text"
    return None


def youtube_url(value: str) -> bool:
    host = urlparse(value.strip()).netloc.lower()
    return "youtube.com" in host or "youtu.be" in host


def human_error(exc: Exception) -> str:
    message = str(exc)
    if "not found" in message.lower():
        return "We could not find the selected file. Please choose it again."
    if "unsupported file type" in message.lower():
        return message
    if "http" in message.lower() or "url" in message.lower():
        return "We could not access this URL. Check the address and try again."
    if "youtube" in message.lower():
        return "The YouTube video could not be accessed or downloaded. Please verify the link."
    return "The document could not be collected. Please check the information and try again."


st.title("Collect a political speech")
st.caption("Add a speech from a website or from your computer. The pipeline handles storage and deduplication automatically.")

with st.form("collection_form"):
    st.subheader("1. Where is your document?")
    source_kind = st.radio(
        "Source",
        options=["url", "youtube", "local"],
        format_func=lambda value: {
            "url": "🌐 Web URL",
            "youtube": "▶️ YouTube",
            "local": "💻 From my computer",
        }[value],
        horizontal=True,
    )

    source_url = None
    uploaded_file = None
    local_path = None

    if source_kind == "url":
        source_url = st.text_input(
            "Document URL",
            placeholder="https://example.com/speech.html or https://example.com/video.mp4",
            help="Paste the public URL of the document or media file.",
        )
    elif source_kind == "youtube":
        source_url = st.text_input(
            "YouTube URL",
            placeholder="https://www.youtube.com/watch?v=...",
        )
    else:
        upload_mode = st.radio(
            "Choose how to provide the local document",
            ["Upload a file", "Enter a local path"],
            horizontal=True,
        )
        if upload_mode == "Upload a file":
            uploaded_file = st.file_uploader(
                "Choose a document",
                type=SUPPORTED_UPLOADS,
                help="Supported: PDF, TXT, HTML, MP3, WAV, M4A, FLAC, MP4, MOV, MKV, AVI, WEBM.",
            )
        else:
            local_path = st.text_input(
                "Local file path",
                placeholder=r"C:\Users\YourName\Documents\speech.mp4",
                help="Use this when the Streamlit application is running on the same computer as the file.",
            )

    st.subheader("2. What type of content is it?")
    content_type = st.radio(
        "Content type",
        options=["text", "audio", "video"],
        format_func=lambda value: TYPE_LABELS[value],
        horizontal=True,
    )

    detected = None
    if uploaded_file:
        detected = detect_type_from_filename(uploaded_file.name)
        if detected:
            st.info(f"Detected: **{TYPE_LABELS[detected]}**. You can change the selection if needed.")

    st.subheader("3. Add optional information")
    col1, col2 = st.columns(2)
    with col1:
        speaker = st.text_input("Speaker / person", placeholder="e.g. Donald Trump")
        title = st.text_input("Title", placeholder="e.g. Inaugural Address")
    with col2:
        publication_date = st.date_input("Publication date", value=None)
        language = st.selectbox("Language", ["Not specified", "English", "French", "Arabic", "Spanish", "Other"])

    notes = st.text_area(
        "Notes",
        placeholder="Optional context, source details, or anything useful for later analysis.",
        height=90,
    )

    submitted = st.form_submit_button("Collect document", type="primary", use_container_width=True)


if submitted:
    errors = []

    if source_kind in {"url", "youtube"}:
        if not source_url or not is_valid_url(source_url):
            errors.append("Please enter a valid URL.")
        elif source_kind == "youtube" and not youtube_url(source_url):
            errors.append("Please enter a valid YouTube URL.")

    if source_kind == "local":
        if uploaded_file is None and not local_path:
            errors.append("Please upload a file or enter a local path.")

    if errors:
        for error in errors:
            st.error(error)
        st.stop()

    metadata = {
        "collection_source": "streamlit_ui",
        "speaker": speaker.strip() or None,
        "title": title.strip() or None,
        "publication_date": publication_date.isoformat() if publication_date else None,
        "language": None if language == "Not specified" else language,
        "notes": notes.strip() or None,
    }
    metadata = {key: value for key, value in metadata.items() if value is not None}

    temporary_path = None
    try:
        if uploaded_file is not None:
            suffix = Path(uploaded_file.name).suffix
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix, prefix="ui_upload_") as temp_file:
                temp_file.write(uploaded_file.getbuffer())
                temporary_path = temp_file.name
            local_path = temporary_path

        youtube_audio_only = source_kind == "youtube" and content_type == "audio"

        with st.status("Collecting your document...", expanded=True) as status:
            st.write("Validating the source")
            st.write("Checking for duplicates")
            result = collect_document(
                source_kind=source_kind,
                content_type=content_type,
                metadata=metadata,
                url=source_url if source_kind != "local" else None,
                local_path=local_path if source_kind == "local" else None,
                youtube_audio_only=youtube_audio_only,
            )
            status.update(label="Collection finished", state="complete", expanded=False)

        if result["status"] == "duplicate":
            st.warning("This document is already in the collection. No duplicate copy was created.")
            st.caption(f"Checksum: `{result['checksum'][:16]}…`")
        else:
            st.success("Document collected successfully and stored in the Bronze layer.")
            left, right = st.columns(2)
            with left:
                st.metric("Document ID", result["doc_id"])
            with right:
                st.metric("Content type", content_type.title())
            st.caption(f"File: {result['file_name']}")

    except Exception as exc:
        st.error(human_error(exc))
        with st.expander("Technical details"):
            st.code(str(exc))
    finally:
        if temporary_path:
            Path(temporary_path).unlink(missing_ok=True)

st.divider()
st.caption("Your submission is stored in Bronze first. Text extraction, transcription, and NLP analysis are handled by later pipeline stages.")
