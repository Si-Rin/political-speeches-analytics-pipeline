"""
History page: previously collected documents — status, source, speaker, date, document ID.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import get_history

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