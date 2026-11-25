"""Shared optional metadata fields for single-document sources. Typed values override what the extractors detect."""
from typing import Any, Dict

import streamlit as st


def render(key_prefix: str) -> Dict[str, Any]:
    with st.expander("Optional metadata (overrides what's automatically detected)"):
        title = st.text_input("Title", key=f"{key_prefix}_title")
        publication_date = st.date_input("Publication date", value=None, key=f"{key_prefix}_date")
        language = st.text_input("Language", placeholder="e.g. English, fr, ar", key=f"{key_prefix}_language")
        notes = st.text_area("Notes", key=f"{key_prefix}_notes")
    return {
        "title": title or None,
        "publication_date": publication_date or None,
        "language": language or None,
        "notes": notes or None,
    }
