"""
Documents page: everything about the collected documents in one place.

  Library     browse / filter / export the metadata of all documents, bulk delete
  Statistics  corpus-level statistics and metadata completeness
  Document    one document: metadata, statistics, text preview, media player, download, analysis results, edit, delete
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt
import pandas as pd
import streamlit as st

import api_client
from components.charts import donut, hbar
from components.gold_views import module_result, render_gold_tabs

st.set_page_config(layout="wide")
st.title("Documents")

VIEWS = ["Library", "Statistics", "Document"]
VIEW_KEY, SELECT_KEY = "documents_view", "documents_selected_doc"
MAX_PLAYER_BYTES = 150 * 1024 * 1024
TEXT_STEP = 5000
ICON = {"success": "✅", "failed": "❌", "pending": "⏳", "not_started": "⚪"}
READING_WPM, SPEAKING_WPM = 200, 130

if st.button("Refresh"):
    st.rerun()


# ---- navigation requests (applied BEFORE the widgets that own these keys are created) ----
if "_documents_pending" in st.session_state:
    view, doc = st.session_state.pop("_documents_pending")
    st.session_state[VIEW_KEY] = view
    if doc is not None:
        st.session_state[SELECT_KEY] = doc
if "_documents_flash" in st.session_state:
    st.success(st.session_state.pop("_documents_flash"))


def go(view: str, doc_id=None):
    st.session_state["_documents_pending"] = (view, doc_id)
    st.rerun()


def fmt_size(n):
    if not n:
        return "—"
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.0f} {unit}" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024


def fmt_minutes(minutes):
    return f"{minutes:.0f} min" if minutes >= 1 else f"{minutes * 60:.0f} s"


def fmt_dt(value):
    ts = pd.to_datetime(value, utc=True, errors="coerce")
    return ts.strftime("%Y-%m-%d %H:%M") if pd.notna(ts) else "—"


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
docs = api_client.get_library()
if not docs:
    st.info("No documents collected yet. Start from the Collection page.")
    st.stop()

df = pd.DataFrame(docs)
df["ingestion_date"] = pd.to_datetime(df["ingestion_date"], utc=True, errors="coerce")
df["publication_date"] = pd.to_datetime(df["publication_date"], errors="coerce")
df["label"] = df.apply(lambda r: f"#{r['doc_id']} — {(r['title'] if isinstance(r['title'], str) and r['title'] else r['source'])[:80]}", axis=1)
doc_ids = df["doc_id"].tolist()

_qp = st.query_params.get("doc_id")
if _qp and str(_qp).isdigit() and int(_qp) in doc_ids and SELECT_KEY not in st.session_state:
    st.session_state[SELECT_KEY], st.session_state[VIEW_KEY] = int(_qp), "Document"
if st.session_state.get(SELECT_KEY) not in doc_ids:
    st.session_state.pop(SELECT_KEY, None)
if st.session_state.get(VIEW_KEY) not in VIEWS:
    st.session_state[VIEW_KEY] = "Library"

top = st.columns([5, 1])
view = top[0].radio("View", VIEWS, horizontal=True, key=VIEW_KEY, label_visibility="collapsed")
if top[1].button("Refresh", key="documents_refresh"):
    st.rerun()


# ===========================================================================
# LIBRARY
# ===========================================================================
def render_library():
    f = st.columns([3, 2, 2, 2])
    query = f[0].text_input("Search", placeholder="title, speaker, file or source…", key="lib_search")
    types = f[1].multiselect("Content type", sorted(df["source_type"].unique()), key="lib_types")
    statuses = f[2].multiselect("Silver status", sorted(df["silver_status"].unique()), key="lib_status")
    langs = f[3].multiselect("Language", sorted(df["language"].dropna().unique()), key="lib_langs")

    view_df = df
    if query:
        q = query.lower()
        hay = view_df[["title", "speaker", "file_name", "source"]].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        view_df = view_df[hay.str.contains(q, regex=False)]
    if types:
        view_df = view_df[view_df["source_type"].isin(types)]
    if statuses:
        view_df = view_df[view_df["silver_status"].isin(statuses)]
    if langs:
        view_df = view_df[view_df["language"].isin(langs)]

    table = pd.DataFrame({
        "ID": view_df["doc_id"], "Title": view_df["title"], "Speaker": view_df["speaker"],
        "Publication date": view_df["publication_date"].dt.date, "Type": view_df["source_type"], "Language": view_df["language"],
        "Words": view_df["word_count"], "Size": view_df["file_size"].map(fmt_size), "Origin": view_df["origin"],
        "Ingested": view_df["ingestion_date"].dt.strftime("%Y-%m-%d %H:%M"),
        "Silver": view_df["silver_status"].map(lambda s: f"{ICON.get(s, '⚪')} {s}"),
        "Gold": view_df.apply(lambda r: f"{r['gold_done']}/{r['gold_total']}", axis=1),
        "Source": view_df["source"],
    })
    st.dataframe(table, hide_index=True, width="stretch", column_config={"Words": st.column_config.NumberColumn(format="%d")})
    bar = st.columns([3, 1])
    bar[0].caption(f"{len(view_df)} of {len(df)} document(s)")
    bar[1].download_button("Download table (CSV)", table.to_csv(index=False).encode("utf-8"), file_name="documents.csv", mime="text/csv", key="lib_csv")

    if len(view_df):
        st.markdown("**Open a document**")
        c1, c2 = st.columns([4, 1])
        pick = c1.selectbox("Document", view_df["doc_id"].tolist(), key="lib_pick",
                            format_func=lambda i: df.loc[df["doc_id"] == i, "label"].iloc[0], label_visibility="collapsed")
        if c2.button("Open", key="lib_open"):
            go("Document", pick)

    with st.expander("Delete documents"):
        st.caption("Deleting removes the document, its text, its analysis results and the stored raw file. "
                   "Its URL becomes unknown again, so it can be collected anew. Topics are computed on the whole corpus: recompute them afterwards.")
        chosen = st.multiselect("Documents to delete", view_df["doc_id"].tolist(), key="lib_delete_ids",
                                format_func=lambda i: df.loc[df["doc_id"] == i, "label"].iloc[0])
        sure = st.checkbox(f"I understand that {len(chosen)} document(s) will be permanently deleted", key="lib_delete_sure")
        if st.button("Delete selected", type="primary", disabled=not (chosen and sure), key="lib_delete_btn"):
            result = api_client.delete_documents(chosen)
            if result:
                extra = f" ({result['storage_errors']} raw file(s) could not be removed from storage)" if result["storage_errors"] else ""
                st.session_state["_documents_flash"] = f"Deleted {result['deleted']} document(s){extra}."
                st.session_state.pop(SELECT_KEY, None)
                go("Library")


# ===========================================================================
# STATISTICS (all documents)
# ===========================================================================
def render_statistics():
    words = df["word_count"].dropna()
    k = st.columns(5)
    k[0].metric("Documents", len(df))
    k[1].metric("Total size", fmt_size(int(df["file_size"].fillna(0).sum())))
    k[2].metric("Total words", f"{int(words.sum()):,}")
    k[3].metric("Average words / document", f"{words.mean():,.0f}" if len(words) else "—")
    k[4].metric("Median words", f"{words.median():,.0f}" if len(words) else "—")
    k = st.columns(5)
    k[0].metric("Languages", df["language"].nunique())
    k[1].metric("Origins", df["origin"].nunique())
    done = int((df["silver_status"] == "success").sum())
    k[2].metric("Silver processed", f"{done} ({done / len(df):.0%})")
    full = int((df["gold_done"] >= df["gold_total"]).sum())
    k[3].metric("Fully analysed", f"{full} ({full / len(df):.0%})")
    dates = df["publication_date"].dropna()
    k[4].metric("Publication range", f"{dates.min():%Y-%m-%d} → {dates.max():%Y-%m-%d}" if len(dates) else "—")

    st.subheader("Pipeline progress")
    silver_counts = df["silver_status"].value_counts()
    gold_state = df.apply(lambda r: "success" if r["gold_done"] >= r["gold_total"] else ("not_started" if r["gold_done"] == 0 else "partial"), axis=1)
    progress = pd.DataFrame([{"layer": "Bronze", "status": "success", "documents": len(df)}]
                            + [{"layer": "Silver", "status": k_, "documents": int(v)} for k_, v in silver_counts.items()]
                            + [{"layer": "Gold", "status": k_, "documents": int(v)} for k_, v in gold_state.value_counts().items()])
    colors = alt.Scale(domain=["success", "partial", "pending", "failed", "not_started"], range=["#2ca02c", "#f2cf5b", "#4c78a8", "#d62728", "#9e9e9e"])
    st.altair_chart(alt.Chart(progress).mark_bar().encode(
        y=alt.Y("layer:N", sort=["Bronze", "Silver", "Gold"], title=None), x=alt.X("documents:Q", stack="zero", title="Documents"),
        color=alt.Color("status:N", scale=colors, legend=alt.Legend(orient="bottom", title=None)), tooltip=["layer", "status", "documents"],
    ).properties(height=150), width="stretch")

    st.subheader("Composition")
    a, b, c = st.columns(3)
    with a:
        st.markdown("**By content type**")
        donut(df["source_type"].value_counts().rename_axis("type").reset_index(name="count"), "type", "count")
    with b:
        st.markdown("**By language**")
        hbar(df["language"].fillna("unknown").value_counts().rename_axis("language").reset_index(name="count"), "language", "count", fmt=",d")
    with c:
        st.markdown("**By origin**")
        hbar(df["origin"].fillna("unknown").value_counts().rename_axis("origin").reset_index(name="count"), "origin", "count", fmt=",d")

    st.subheader("Length and timeline")
    a, b = st.columns(2)
    with a:
        st.markdown("**Words per document**")
        if len(words):
            st.altair_chart(alt.Chart(pd.DataFrame({"words": words})).mark_bar().encode(
                x=alt.X("words:Q", bin=alt.Bin(maxbins=25), title="Words"), y=alt.Y("count():Q", title="Documents")).properties(height=240), width="stretch")
        else:
            st.caption("No word counts yet.")
    with b:
        st.markdown("**Documents ingested per day**")
        per_day = df.assign(day=df["ingestion_date"].dt.date).groupby("day").size().reset_index(name="documents")
        st.altair_chart(alt.Chart(per_day).mark_bar().encode(x=alt.X("day:T", title=None), y=alt.Y("documents:Q", title=None),
                                                             tooltip=["day:T", "documents"]).properties(height=240), width="stretch")
    if len(dates):
        st.markdown("**Documents by publication year**")
        per_year = dates.dt.year.value_counts().sort_index().rename_axis("year").reset_index(name="documents")
        st.altair_chart(alt.Chart(per_year).mark_bar().encode(x=alt.X("year:O", title=None), y=alt.Y("documents:Q", title=None),
                                                              tooltip=["year", "documents"]).properties(height=200), width="stretch")

    st.subheader("Metadata completeness")
    st.caption("Share of documents that have each field filled. Missing values can be completed from the Document view (Edit).")
    comp = pd.DataFrame([{"field": name, "filled": df[col].notna().mean() if col != "title" else (df[col].fillna("").str.strip() != "").mean()}
                         for name, col in (("Title", "title"), ("Speaker", "speaker"), ("Publication date", "publication_date"),
                                           ("Language", "language"), ("Word count", "word_count"))])
    hbar(comp, "field", "filled", fmt=".0%", color="#59a14f")

    st.subheader("Longest documents")
    top_docs = df.sort_values("word_count", ascending=False).head(10)
    st.dataframe(pd.DataFrame({"ID": top_docs["doc_id"], "Title": top_docs["title"].fillna(top_docs["source"]), "Words": top_docs["word_count"],
                               "Type": top_docs["source_type"]}), hide_index=True, width="stretch")


# ===========================================================================
# DOCUMENT (one document)
# ===========================================================================
def prepared(key: str, doc_id: int):
    """Download buttons need bytes: fetch on demand (a raw file can be hundreds of MB), keep them for this document."""
    cached = st.session_state.get(key)
    return cached[1] if cached and cached[0] == doc_id else None


def render_document():
    doc_id = st.selectbox("Document", doc_ids, key=SELECT_KEY, format_func=lambda i: df.loc[df["doc_id"] == i, "label"].iloc[0],
                          label_visibility="collapsed")
    detail = api_client.get_document_detail(doc_id)
    if not detail:
        return
    status = api_client.get_status(doc_id)
    raw_meta = detail.get("raw_metadata") or {}
    silver_ok = detail["silver_status"] == "success"
    media = detail["source_type"] in ("audio", "video")

    st.header(detail.get("title") or detail["file_name"] or f"Document #{doc_id}")
    st.caption(f"{detail['source_type'].capitalize()} · #{doc_id} · {ICON.get(detail['silver_status'], '⚪')} Silver {detail['silver_status']} · "
               f"Gold {detail['gold_done']}/{detail['gold_total']} · ingested {fmt_dt(detail['ingestion_date'])}")
    if detail["source"].startswith("http"):
        st.markdown(f"[Open the original source ↗]({detail['source']})")
    if detail.get("silver_error"):
        st.error(f"Silver error: {detail['silver_error']}")

    # ---- actions ----
    a = st.columns(3)
    if a[0].button("Prepare raw file", key=f"prep_raw_{doc_id}"):
        data = api_client.download_document(doc_id)
        if data:
            st.session_state["dl_raw"] = (doc_id, data)
    raw = prepared("dl_raw", doc_id)
    if raw:
        a[0].download_button("Download raw file", raw, file_name=detail["file_name"] or f"document_{doc_id}", key=f"dl_raw_{doc_id}")
    if silver_ok:
        if a[1].button("Prepare text", key=f"prep_txt_{doc_id}"):
            data = api_client.download_transcript(doc_id)
            if data:
                st.session_state["dl_txt"] = (doc_id, data)
        txt = prepared("dl_txt", doc_id)
        if txt:
            a[1].download_button("Download text (.txt)", txt, file_name=f"document_{doc_id}.txt", mime="text/plain", key=f"dl_txt_{doc_id}")
    else:
        a[1].caption("Text available once Silver succeeds.")
    if status and any(m["done"] for m in status["gold_modules"]):
        results = {m["module"]: m["result"] for m in status["gold_modules"] if m["done"]}
        a[2].download_button("Download analysis (JSON)", json.dumps(results, indent=2, default=str).encode("utf-8"),
                             file_name=f"document_{doc_id}_analysis.json", mime="application/json", key=f"dl_an_{doc_id}")

    tabs = st.tabs(["Overview", "Text", "Media" if media else "Raw file", "Analysis", "Edit & delete"])

    # ---- Overview: metadata + statistics ----
    with tabs[0]:
        words, chars = detail.get("word_count"), detail.get("char_count")
        syn = (module_result(status, "syntactic") if status else None) or {}
        lex = (module_result(status, "lexical") if status else None) or {}
        stats = (syn.get("speech_stats") or {})
        duration = raw_meta.get("duration")
        st.subheader("Statistics")
        m = st.columns(5)
        m[0].metric("Words", f"{words:,}" if words else "—")
        m[1].metric("Characters", f"{chars:,}" if chars else "—")
        m[2].metric("Avg word length", f"{chars / words:.1f}" if words and chars else "—", help="Characters per word, spaces included")
        m[3].metric("Reading time", fmt_minutes(words / READING_WPM) if words else "—", help=f"At {READING_WPM} words per minute")
        m[4].metric("Speaking time", fmt_minutes(words / SPEAKING_WPM) if words else "—", help=f"At {SPEAKING_WPM} words per minute")
        m = st.columns(5)
        m[0].metric("Media duration", fmt_minutes(duration / 60) if isinstance(duration, (int, float)) and duration else "—")
        m[1].metric("Sentences", f"{stats['sentence_count']:,}" if stats.get("sentence_count") else "—")
        m[2].metric("Avg sentence length", f"{stats['avg_sentence_length']:.1f} words" if stats.get("avg_sentence_length") else "—")
        ttr = (lex.get("lexical_diversity") or {}).get("ttr")
        m[3].metric("Type-token ratio", f"{ttr:.3f}" if ttr is not None else "—")
        corpus_words = df["word_count"].dropna()
        if words and len(corpus_words) > 1:
            shorter = (corpus_words < words).mean()
            m[4].metric("Longer than", f"{shorter:.0%} of documents", help=f"Corpus average: {corpus_words.mean():,.0f} words")
        else:
            m[4].metric("Longer than", "—")

        st.subheader("Metadata")
        fields = [
            ("Title", detail.get("title")), ("Speaker", detail.get("speaker")), ("Publication date", detail.get("publication_date")),
            ("Language", detail.get("language")), ("Content type", detail["source_type"]), ("Origin", detail.get("origin")),
            ("File name", detail.get("file_name")), ("MIME type", detail.get("mime_type")), ("File size", fmt_size(detail.get("file_size"))),
            ("Source", detail["source"]), ("Ingested", fmt_dt(detail["ingestion_date"])), ("Processed (Silver)", fmt_dt(detail.get("processing_date"))),
            ("Collection run", detail.get("first_run_id")), ("Checksum (SHA-256)", detail["checksum"]), ("Storage path", detail["storage_path"]),
        ]
        st.dataframe(pd.DataFrame([{"Field": k_, "Value": "—" if v in (None, "") else str(v)} for k_, v in fields]), hide_index=True, width="stretch")
        extra = {k_: v for k_, v in raw_meta.items() if k_ not in ("collection", "source_name") and v not in (None, "", [], {})}
        if extra:
            with st.expander(f"Source metadata ({len(extra)} fields)"):
                st.dataframe(pd.DataFrame([{"Field": k_, "Value": (", ".join(map(str, v)) if isinstance(v, list) else str(v))[:400]}
                                           for k_, v in extra.items()]), hide_index=True, width="stretch")

    # ---- Text preview ----
    with tabs[1]:
        if not silver_ok:
            st.info("The text is available once Silver processing has succeeded.")
        else:
            length_key = f"text_len_{doc_id}"
            length = st.session_state.get(length_key, TEXT_STEP)
            preview = api_client.get_text_preview(doc_id, 0, length)
            if preview:
                st.text_area("Text", preview["text"], height=420, disabled=True, label_visibility="collapsed", key=f"text_{doc_id}_{length}")
                shown = min(length, preview["total_chars"])
                st.caption(f"Showing {shown:,} of {preview['total_chars']:,} characters")
                if preview["has_more"] and st.button("Show more", key=f"more_{doc_id}"):
                    st.session_state[length_key] = length + 2 * TEXT_STEP
                    st.rerun()

    # ---- Media player / raw file ----
    with tabs[2]:
        size = detail.get("file_size") or 0
        if media:
            if size > MAX_PLAYER_BYTES:
                st.info(f"This file is {fmt_size(size)}, too large to play here. Use “Download raw file” above.")
            else:
                if st.button("Load player", key=f"player_{doc_id}"):
                    data = api_client.download_document(doc_id)
                    if data:
                        st.session_state["player"] = (doc_id, data)
                clip = prepared("player", doc_id)
                if clip:
                    (st.video if detail["source_type"] == "video" else st.audio)(clip, format=detail.get("mime_type") or None)
        else:
            if size > 5 * 1024 * 1024 or not (detail.get("mime_type") or "").startswith("text"):
                st.info("This file cannot be previewed here. Use “Download raw file” above.")
            elif st.button("Show raw content", key=f"rawview_{doc_id}"):
                data = api_client.download_document(doc_id)
                if data:
                    st.code(data[:5000].decode("utf-8", errors="replace"), language="html", wrap_lines=True)
                    st.caption(f"First 5,000 of {len(data):,} bytes")

    # ---- Analysis ----
    with tabs[3]:
        if silver_ok and status:
            render_gold_tabs(status)
        else:
            st.info("Analysis results appear after Silver processing succeeds.")

    # ---- Edit & delete ----
    with tabs[4]:
        st.subheader("Edit metadata")
        if not silver_ok and detail["silver_status"] == "not_started":
            st.info("Metadata can be edited once Silver has processed the document.")
        else:
            with st.form(f"edit_{doc_id}"):
                title = st.text_input("Title", value=detail.get("title") or "")
                speaker = st.text_input("Speaker", value=detail.get("speaker") or "")
                pub = detail.get("publication_date")
                publication_date = st.date_input("Publication date", value=pd.to_datetime(pub).date() if pub else None, min_value=pd.Timestamp("1700-01-01").date())
                language = st.text_input("Language", value=detail.get("language") or "", help="Language code, for example en")
                if st.form_submit_button("Save metadata", type="primary"):
                    ok = api_client.update_document_metadata(doc_id, title=title.strip() or None, speaker=speaker.strip() or None,
                                                             publication_date=publication_date.isoformat() if publication_date else None,
                                                             language=language.strip() or None)
                    if ok:
                        st.session_state["_documents_flash"] = "Metadata saved."
                        st.rerun()
            st.caption("Re-running Silver processing on this document can overwrite title and date with the values found in the source.")
        st.divider()
        st.subheader("Delete this document")
        st.caption("Removes the document, its text, its analysis results and the stored raw file. Its URL can then be collected again. "
                   "Recompute the topics afterwards, they are corpus-level.")
        sure = st.checkbox("I understand this is permanent", key=f"del_sure_{doc_id}")
        if st.button("Delete document", type="primary", disabled=not sure, key=f"del_{doc_id}"):
            result = api_client.delete_documents([doc_id])
            if result and result["deleted"]:
                st.session_state["_documents_flash"] = f"Document #{doc_id} deleted."
                st.session_state.pop(SELECT_KEY, None)
                go("Library")


{"Library": render_library, "Statistics": render_statistics, "Document": render_document}[view]()
