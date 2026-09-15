"""
Pipeline Status page: Bronze / Silver / Gold processing state for each document.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import json
import streamlit as st

from api_client import get_all_statuses

st.set_page_config(layout="wide")

st.title("Pipeline Status")

if st.button("Refresh"):
    st.rerun()

def format_result(result):
    """Format a Gold JSON/JSONB result for compact table display."""
    if result is None:
        return "pending"

    if isinstance(result, (dict, list)):
        return json.dumps(result, ensure_ascii=False)

    return str(result)

statuses = get_all_statuses()

if not statuses:
    st.info("No documents to show yet.")
else:
    overview_rows = []

    for status in statuses:
        row = {
            "Document ID": status["doc_id"],
            "Bronze": "success",
            "Silver": status["silver_status"],
            "Gold": status["gold_status"],
        }

        for module in status["gold_modules"]:
            row[module["module"].capitalize()] = format_result(
                module.get("result")
            )

        overview_rows.append(row)

    st.subheader("Documents and Gold results")

    st.dataframe(
        pd.DataFrame(overview_rows),
        width="stretch",
        hide_index=True,
    )

    doc_ids = [s["doc_id"] for s in statuses]
    selected = st.selectbox("Document ID", doc_ids)
    selected_status = next(s for s in statuses if s["doc_id"] == selected)

    if selected_status.get("silver_error"):
        st.error(f"Silver error: {selected_status['silver_error']}")

    for module in selected_status["gold_modules"]:
        st.markdown(f"### {module['module'].capitalize()}")

        result = module.get("result")

        if result is None:
            st.info("Pending")
        elif isinstance(result, (dict, list)):
            st.json(result)
        else:
            st.write(result)