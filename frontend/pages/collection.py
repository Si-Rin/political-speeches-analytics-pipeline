"""
Collection page: choose a mode, choose a source, fill in its form.

- "Single document": one item at a time, through POST /runs (source-specific request models on the backend).
  The source selector lives outside any st.form so picking a different source redraws the right fields.
- "Archive collection": today this is only `web_crawl`, kept under "Advanced" until phases 4a-4d add the
  research sources (Miller Center, UCSB, YouTube collection, Internet Archive collection).

A full Runs view (status, discovery log, cost hint, cancel) is phase 5 — for now, a successful submission just
names the run_id and points to History.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from components import archive_forms, single_forms

st.set_page_config(layout="wide")
st.title("Add a source")

mode = st.radio("Mode", ["Single document", "Archive collection"], horizontal=True, key="collection_mode")
st.divider()

if mode == "Single document":
    source = st.radio("Source", list(single_forms.RENDERERS), horizontal=True, key="collection_single_source")
    single_forms.RENDERERS[source]()
else:
    source = st.radio("Source", list(archive_forms.RENDERERS), horizontal=True, key="collection_archive_source")
    archive_forms.RENDERERS[source]()
    st.caption("YouTube collections and Internet Archive collections are not available yet.")
