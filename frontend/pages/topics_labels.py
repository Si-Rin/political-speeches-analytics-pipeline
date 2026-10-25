"""
Topics & Labels page: BERTopic topic breakdown, cross-validation against the zero-shot taxonomy, and the overall category distribution.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import get_labels, get_topics, get_topics_run_status, start_topics_run

st.set_page_config(layout="wide")
st.title("Topics & Labels")

if st.button("Refresh"):
    st.rerun()


# ---------------------------------------------------------------------------
# Topic modeling control (replaces running gold_topics.py by hand)
# ---------------------------------------------------------------------------
RUN_STATES = {"queued": "⏳ Waiting to start", "running": "🔄 Running", "completed": "✅ Completed",
              "failed": "❌ Failed", "cancelled": "⚪ Cancelled", "idle": "⚪ Never run"}
REFRESH_SECONDS = 5

first_status = get_topics_run_status()
keep_refreshing = bool(first_status) and first_status["state"] in ("queued", "running")


@st.fragment(run_every=f"{REFRESH_SECONDS}s" if keep_refreshing else None)
def topics_control():
    status = get_topics_run_status()
    if not status:
        return
    running = status["state"] in ("queued", "running")
    if keep_refreshing and not running:
        st.rerun()                        # finished: refresh the whole page so the new topics appear

    st.subheader("Compute topics")
    st.caption("Topic modeling groups the whole corpus into themes. It needs enough processed documents and is recomputed "
               "over all of them each time, so run it after you have collected a good batch.")
    need = status["min_docs"]
    c1, c2 = st.columns(2)
    for col, label, n in ((c1, "Long speeches (500+ words)", status["long_form_docs"]),
                          (c2, "Short posts (under 500 words)", status["short_form_docs"])):
        col.metric(label, f"{n} / {need} needed")
        col.progress(min(n / need, 1.0))
    st.markdown(f"**Last run:** {RUN_STATES.get(status['state'], status['state'])}"
                + (f" · started {str(status['started_at'])[:19].replace('T', ' ')}" if status.get("started_at") else "")
                + (f" · finished {str(status['finished_at'])[:19].replace('T', ' ')}" if status.get("finished_at") else ""))
    if status["state"] == "failed" and status.get("message"):
        st.error(status["message"])
    if running:
        st.info(f"Topic modeling in progress (this can take several minutes). Refreshing every {REFRESH_SECONDS} seconds.")
    elif not status["can_run"]:
        st.warning(f"Not enough documents yet: at least {need} processed documents are needed in the long-speech or the "
                   "short-post group. Collect more documents first.")
    if st.button("Compute topics now", type="primary", disabled=running or not status["can_run"], key="start_topics"):
        if start_topics_run():
            st.rerun()


topics_control()
st.divider()

st.subheader("BERTopic topics")
st.caption(
    "The corpus is modeled as two separate sets — long-form speeches and short-form posts."
    "Topic -1 is BERTopic's outlier bucket for documents that cannot be clustered."
)

topics_data = get_topics()

if not topics_data or not topics_data.get("topics"):
    st.info("No topics computed yet. Use “Compute topics now” above once enough documents are processed.")
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