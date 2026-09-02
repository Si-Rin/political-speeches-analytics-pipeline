"""
Pipeline Status page: Bronze / Silver / Gold processing state for each document.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import get_all_statuses

st.title("Pipeline Status")

if st.button("Refresh"):
    st.rerun()

statuses = get_all_statuses()

if not statuses:
    st.info("No documents to show yet.")
else:
    overview_rows = [
        {
            "Document ID": s["doc_id"],
            "Bronze": "✅ success",
            "Silver": s["silver_status"],
            "Gold": s["gold_status"],
        }
        for s in statuses
    ]
    st.dataframe(pd.DataFrame(overview_rows), width='stretch', hide_index=True)

    st.subheader("Gold module detail")
    doc_ids = [s["doc_id"] for s in statuses]
    selected = st.selectbox("Document ID", doc_ids)
    selected_status = next(s for s in statuses if s["doc_id"] == selected)

    if selected_status.get("silver_error"):
        st.error(f"Silver error: {selected_status['silver_error']}")

    module_cols = st.columns(len(selected_status["gold_modules"]))
    for col, module in zip(module_cols, selected_status["gold_modules"]):
        with col:
            st.metric(module["module"], "done" if module["done"] else "pending")