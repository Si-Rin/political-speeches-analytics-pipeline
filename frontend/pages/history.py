"""
History page: previously collected documents — status, source, speaker, date, document ID.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

import api_client
from api_client import get_history

st.set_page_config(layout="wide")

st.title("History")

col1, col2 = st.columns([3, 1])
with col1:
    source_type = st.selectbox("Filter by content type", ["All", "text", "video", "audio"])
with col2:
    st.write("")
    st.write("")
    refresh = st.button("Refresh")

documents = get_history(source_type=source_type)

if not documents:
    st.info("No documents collected yet.")
else:
    df = pd.DataFrame(documents)
    df = df.rename(columns={
        "doc_id": "Document ID",
        "source": "Source",
        "source_type": "Content type",
        "speaker": "Speaker",
        "title": "Title",
        "publication_date": "Publication date",
        "ingestion_date": "Ingested at",
        "silver_status": "Processing status",
    })
    st.dataframe(df, width='stretch', hide_index=True)
    st.caption(f"{len(documents)} document(s)")

    st.divider()
    st.subheader("Download a document")
    labels = {d["doc_id"]: f"#{d['doc_id']} — {d.get('title') or d['source']}"[:110] for d in documents}
    doc_id = st.selectbox("Document", list(labels), format_func=labels.get, key="history_download_doc")
    selected = next(d for d in documents if d["doc_id"] == doc_id)
    c1, c2 = st.columns(2)
    if c1.button("Prepare raw file", key="prep_raw"):
        data = api_client.download_document(doc_id)
        if data:
            st.session_state["dl_raw"] = (doc_id, data)
    raw = st.session_state.get("dl_raw")
    if raw and raw[0] == doc_id:
        c1.download_button("Download raw file", raw[1], file_name=f"document_{doc_id}", key="dl_raw_btn")
    if selected["silver_status"] == "success":
        if c2.button("Prepare transcript / text", key="prep_txt"):
            data = api_client.download_transcript(doc_id)
            if data:
                st.session_state["dl_txt"] = (doc_id, data)
        txt = st.session_state.get("dl_txt")
        if txt and txt[0] == doc_id:
            c2.download_button("Download .txt", txt[1], file_name=f"document_{doc_id}.txt", mime="text/plain", key="dl_txt_btn")
    else:
        c2.caption(f"Text not available yet (processing status: {selected['silver_status']}).")
    