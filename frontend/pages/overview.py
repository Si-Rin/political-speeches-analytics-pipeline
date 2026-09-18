"""
Analytics Overview page: corpus-level statistics.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import get_overview

st.set_page_config(layout="wide")
st.title("Analytics Overview")

if st.button("Refresh"):
    st.rerun()

data = get_overview()

if not data:
    st.info("No data available yet.")
else:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Total documents", data["total_documents"])
    col2.metric("Silver success", data["silver_success"])
    col3.metric("Silver failed", data["silver_failed"])
    date_range = (
        f"{data['date_min']} → {data['date_max']}" if data.get("date_min") else "—"
    )
    col4.metric("Date range", date_range)

    st.divider()

    col_a, col_b = st.columns(2)
    with col_a:
        st.subheader("By content type")
        df_type = pd.DataFrame(data["by_source_type"])
        if not df_type.empty:
            st.bar_chart(df_type.set_index("source_type")["count"])
    with col_b:
        st.subheader("By origin")
        df_origin = pd.DataFrame(data["by_origin"])
        if not df_origin.empty:
            st.bar_chart(df_origin.set_index("source_name")["count"])

    st.divider()

    if data.get("word_count_stats"):
        st.subheader("Word count distribution")
        wc = data["word_count_stats"]
        cols = st.columns(5)
        cols[0].metric("Min", wc["min"])
        cols[1].metric("Max", wc["max"])
        cols[2].metric("Mean", f"{wc['mean']:.0f}")
        cols[3].metric("Median", f"{wc['median']:.0f}")
        cols[4].metric("Std dev", f"{wc['stddev']:.0f}")
        st.caption(
            "Mean well above median indicates a right-skewed distribution "
            "(many short posts, a smaller number of long speeches)."
        )

    st.divider()

    st.subheader("Documents over time")
    df_time = pd.DataFrame(data["documents_over_time"])
    if not df_time.empty:
        st.bar_chart(df_time.set_index("month")["count"])
    else:
        st.info("No dated documents yet.")