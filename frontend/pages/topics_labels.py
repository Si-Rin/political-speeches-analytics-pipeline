"""
Topics & Labels page: BERTopic topic breakdown, cross-validation against the zero-shot taxonomy, and the overall category distribution.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import get_topics, get_labels

st.set_page_config(layout="wide")
st.title("Topics & Labels")

if st.button("Refresh"):
    st.rerun()

st.subheader("BERTopic topics")
st.caption(
    "The corpus is modeled as two separate sets — long-form speeches and short-form posts."
    "Topic -1 is BERTopic's outlier bucket for documents that cannot be clustered."
)

topics_data = get_topics()

if not topics_data or not topics_data.get("topics"):
    st.info("No topics computed yet — run gold_topics.py.")
else:
    for corpus in ["long_form", "short_form"]:
        corpus_topics = [t for t in topics_data["topics"] if t["corpus"] == corpus]
        if not corpus_topics:
            continue
        st.markdown(f"**{corpus.replace('_', '-').title()} corpus**")
        rows = []
        for t in sorted(corpus_topics, key=lambda x: x["topic_id"]):
            label = "outlier (-1)" if t["is_outlier"] else str(t["topic_id"])
            rows.append({
                "Topic": label,
                "Documents": t["n_docs"],
                "Keywords": ", ".join(t["keywords"][:8]) if t["keywords"] else "—",
            })
        st.dataframe(pd.DataFrame(rows), width='stretch', hide_index=True)

    st.divider()
    st.subheader("Cross-validation against the label taxonomy")
    st.caption(
        "For each topic, the dominant zero-shot label across its documents lift close to 1 means the label is just common everywhere (expected for broad, generic topics)"
        "a high lift means a genuine, topic-specific association."
    )
    if topics_data.get("validation"):
        val_rows = [
            {
                "Corpus": v["corpus"],
                "Topic": "outlier (-1)" if v["topic_id"] == -1 else v["topic_id"],
                "Documents": v["n_docs"],
                "Dominant label": v["dominant_label"],
                "Purity": v["purity"],
                "Lift": v["lift"] if v["lift"] is not None else "—",
            }
            for v in topics_data["validation"]
        ]
        st.dataframe(pd.DataFrame(val_rows), width='stretch', hide_index=True)
    else:
        st.info("No validation data available (need both topics and labels computed).")

st.divider()
st.subheader("Category distribution (zero-shot classification)")

labels_data = get_labels()
if not labels_data:
    st.info("No label data available yet.")
else:
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("**Long-form**")
        df_lf = pd.DataFrame(labels_data.get("long_form", []))
        if not df_lf.empty:
            st.bar_chart(df_lf.set_index("label")["pct"])
    with col2:
        st.markdown("**Short-form**")
        df_sf = pd.DataFrame(labels_data.get("short_form", []))
        if not df_sf.empty:
            st.bar_chart(df_sf.set_index("label")["pct"])