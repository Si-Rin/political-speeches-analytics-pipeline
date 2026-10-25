"""
Pipeline Status page: Bronze / Silver / Gold processing state per document, and the per-document Gold results.

Only PER-DOCUMENT Gold modules are shown here (lexical, syntactic, entities, sentiment, emotions, labels).
Topics and keywords are corpus-level (BERTopic is fitted on the whole corpus), so they live on the Topics & Labels page.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import altair as alt
import pandas as pd
import streamlit as st

from api_client import get_all_statuses, get_history
from components.charts import hbar

st.set_page_config(layout="wide")
st.title("Pipeline Status")

PER_DOC_MODULES = ["lexical", "syntactic", "entities", "sentiment", "emotions", "labels"]   # topics/keywords are corpus-level
ICON = {"success": "✅ success", "pending": "⏳ pending", "failed": "❌ failed", "partial": "🟡 partial", "not_started": "⚪ not started"}

if st.button("Refresh"):
    st.rerun()


# ---------------------------------------------------------------------------
# Small chart helpers (altair): horizontal sorted bars, readable labels
# ---------------------------------------------------------------------------
def scores_df(scores: dict, label: str = "label") -> pd.DataFrame:
    return pd.DataFrame([{label: k, "score": v} for k, v in (scores or {}).items()]).sort_values("score", ascending=False) \
        if scores else pd.DataFrame(columns=[label, "score"])


def module_result(status: dict, name: str):
    for m in status["gold_modules"]:
        if m["module"] == name:
            return m.get("result")
    return None


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------
statuses = get_all_statuses()
if not statuses:
    st.info("No documents to show yet.")
    st.stop()

titles = {d["doc_id"]: (d.get("title") or d.get("source") or "") for d in (get_history() or [])}


def per_doc_gold(status: dict) -> tuple[int, str]:
    done = sum(1 for m in status["gold_modules"] if m["module"] in PER_DOC_MODULES and m["done"])
    overall = "success" if done == len(PER_DOC_MODULES) else ("not_started" if done == 0 else "partial")
    return done, overall


# ---------------------------------------------------------------------------
# 1. Corpus-level progress
# ---------------------------------------------------------------------------
gold_overall = [per_doc_gold(s)[1] for s in statuses]
silver_counts = pd.Series([s["silver_status"] for s in statuses]).value_counts()
gold_counts = pd.Series(gold_overall).value_counts()

m = st.columns(4)
m[0].metric("Documents", len(statuses))
m[1].metric("Silver done", int(silver_counts.get("success", 0)))
m[2].metric("Gold complete", int(gold_counts.get("success", 0)))
m[3].metric("Needs attention", int(silver_counts.get("failed", 0)))

progress = pd.DataFrame(
    [{"layer": "Bronze", "status": "success", "documents": len(statuses)}]
    + [{"layer": "Silver", "status": k, "documents": int(v)} for k, v in silver_counts.items()]
    + [{"layer": "Gold", "status": k, "documents": int(v)} for k, v in gold_counts.items()]
)
colors = alt.Scale(domain=["success", "partial", "pending", "failed", "not_started"], range=["#2ca02c", "#f2cf5b", "#4c78a8", "#d62728", "#9e9e9e"])
st.altair_chart(
    alt.Chart(progress).mark_bar().encode(
        y=alt.Y("layer:N", sort=["Bronze", "Silver", "Gold"], title=None),
        x=alt.X("documents:Q", stack="zero", title="Documents"),
        color=alt.Color("status:N", scale=colors, legend=alt.Legend(orient="bottom", title=None)),
        tooltip=["layer", "status", "documents"],
    ).properties(height=150),
    width="stretch",
)


# ---------------------------------------------------------------------------
# 2. Documents table: statuses + readable per-document summary (no raw JSON)
# ---------------------------------------------------------------------------
rows = []
for s in statuses:
    done, overall = per_doc_gold(s)
    sentiment = module_result(s, "sentiment") or {}
    emotions = module_result(s, "emotions") or {}
    labels = module_result(s, "labels") or {}
    lex = module_result(s, "lexical") or {}
    rows.append({
        "ID": s["doc_id"],
        "Title": (titles.get(s["doc_id"]) or "")[:70],
        "Bronze": ICON["success"],
        "Silver": ICON.get(s["silver_status"], s["silver_status"]),
        "Gold": f"{ICON[overall]} ({done}/{len(PER_DOC_MODULES)})",
        "Sentiment": sentiment.get("label") or "—",
        "Dominant emotion": emotions.get("dominant_emotion") or "—",
        "Labels": ", ".join(labels.get("labels", [])) or "—",
        "Words": lex.get("total_words"),
    })

st.subheader("Documents")
st.dataframe(pd.DataFrame(rows), width="stretch", hide_index=True, column_config={"Words": st.column_config.NumberColumn(format="%d")})

# ---------------------------------------------------------------------------
# 3. One document in detail
# ---------------------------------------------------------------------------
st.divider()
st.subheader("Document details")
doc_ids = [s["doc_id"] for s in statuses]
SELECT_KEY = "status_selected_doc"        # set by the Runs page ("Open in Pipeline Status") or by ?doc_id=
_qp = st.query_params.get("doc_id")
if _qp and str(_qp).isdigit() and int(_qp) in doc_ids and SELECT_KEY not in st.session_state:
    st.session_state[SELECT_KEY] = int(_qp)
if st.session_state.get(SELECT_KEY) not in doc_ids:
    st.session_state.pop(SELECT_KEY, None)
selected = st.selectbox("Document", doc_ids, key=SELECT_KEY, format_func=lambda i: f"#{i} — {titles.get(i, '')[:80]}")
status = next(s for s in statuses if s["doc_id"] == selected)

if status.get("silver_error"):
    st.error(f"Silver error: {status['silver_error']}")
if status["silver_status"] != "success":
    st.info(f"Silver status: {ICON.get(status['silver_status'], status['silver_status'])}. Gold results appear after Silver succeeds.")

tabs = st.tabs([n.capitalize() for n in PER_DOC_MODULES])

# ---- Lexical ----
with tabs[0]:
    lex = module_result(status, "lexical")
    if not lex:
        st.info("Pending")
    else:
        c = st.columns(3)
        c[0].metric("Words", f"{lex.get('total_words', 0):,}")
        div = lex.get("lexical_diversity") or {}
        c[1].metric("Type-token ratio", f"{div['ttr']:.3f}" if div.get("ttr") is not None else "—")
        c[2].metric("MTLD", f"{div['mtld']:.1f}" if div.get("mtld") is not None else "—")

        a, b = st.columns(2)
        with a:
            st.markdown("**Pronoun use** (per 1,000 words)")
            hbar(pd.DataFrame([{"pronoun": k.replace("_", " "), "per_1000": v} for k, v in (lex.get("pronoun_ratios") or {}).items()]), "pronoun", "per_1000")
        with b:
            st.markdown("**Most frequent content words**")
            hbar(pd.DataFrame(lex.get("top_content_words") or []), "word", "count", fmt=",d")

        opp = lex.get("oppositions") or []
        if opp:
            st.markdown("**Binary oppositions** (occurrences of each side)")
            long = pd.DataFrame([{"pair": f"{o['pair'][0]} vs {o['pair'][1]}", "word": w, "count": n} for o in opp for w, n in zip(o["pair"], o["counts"])])
            st.altair_chart(
                alt.Chart(long).mark_bar().encode(
                    y=alt.Y("pair:N", title=None), x=alt.X("count:Q", title=None),
                    yOffset="word:N", color=alt.Color("word:N", legend=None), tooltip=["word", "count"],
                ).properties(height=max(120, 46 * len(opp))),
                width="stretch",
            )

        a, b, c3 = st.columns(3)
        for col, key, title in ((a, "bigrams", "Top bigrams"), (b, "trigrams", "Top trigrams"),
                                (c3, "repetitions", "Repeated sentence openings (anaphora)")):
            with col:
                st.markdown(f"**{title}**")
                data = lex.get(key) or []
                if data:
                    st.dataframe(pd.DataFrame(data), hide_index=True, width="stretch")
                else:
                    st.caption("None found.")

# ---- Syntactic ----
with tabs[1]:
    syn = module_result(status, "syntactic")
    if not syn:
        st.info("Pending")
    else:
        stats = {k: v for k, v in (syn.get("speech_stats") or {}).items() if isinstance(v, (int, float))}
        if stats:
            cols = st.columns(min(len(stats), 5))
            for i, (k, v) in enumerate(stats.items()):
                cols[i % len(cols)].metric(k.replace("_", " ").capitalize(), f"{v:,.1f}" if isinstance(v, float) else f"{v:,}")
        a, b, c = st.columns(3)
        with a:
            st.markdown("**Part-of-speech mix**")
            hbar(pd.DataFrame([{"pos": k, "value": v} for k, v in (syn.get("pos_distribution") or {}).items()]), "pos", "value")
        with b:
            st.markdown("**Verb tense** (% of verbs)")
            tense = pd.DataFrame([{"tense": k.replace("_", "-"), "percent": v} for k, v in (syn.get("verb_tense") or {}).items()])
            if not tense.empty:
                st.altair_chart(
                    alt.Chart(tense).mark_arc(innerRadius=50).encode(
                        theta="percent:Q", color=alt.Color("tense:N", legend=alt.Legend(orient="bottom", title=None)),
                        tooltip=["tense", "percent"]).properties(height=260), width="stretch",
                )
        with c:
            st.markdown("**Modality** (per 1,000 words)")
            hbar(pd.DataFrame([{"modality": k.replace("_modality", ""), "per_1000": v} for k, v in (syn.get("modality") or {}).items()]), "modality", "per_1000")

# ---- Entities ----
with tabs[2]:
    ent = module_result(status, "entities")
    if not ent:
        st.info("Pending")
    else:
        groups = [g for g, v in ent.items() if isinstance(v, list) and v]
        c = st.columns(2)
        c[0].metric("Entity mentions", f"{ent.get('total_entities', 0):,}")
        c[1].metric("Entity types", len(ent.get("by_label") or {}))
        if groups:
            cols = st.columns(min(len(groups), 3))
            for i, g in enumerate(groups):
                with cols[i % len(cols)]:
                    st.markdown(f"**{g.capitalize()}**")
                    hbar(pd.DataFrame(ent[g][:10]), "text", "count", fmt=",d")
        if ent.get("by_label"):
            st.markdown("**Mentions by spaCy label**")
            hbar(pd.DataFrame([{"label": k, "count": v} for k, v in ent["by_label"].items()]), "label", "count", fmt=",d")

# ---- Sentiment ----
with tabs[3]:
    sent = module_result(status, "sentiment")
    if not sent:
        st.info("Pending")
    else:
        st.metric("Dominant sentiment", (sent.get("label") or "—").capitalize())
        hbar(scores_df(sent.get("scores")), "label", "score", fmt=".1%")

# ---- Emotions ----
with tabs[4]:
    emo = module_result(status, "emotions")
    if not emo:
        st.info("Pending")
    else:
        st.metric("Dominant emotion", (emo.get("dominant_emotion") or "—").capitalize())
        hbar(scores_df(emo.get("scores"), "emotion"), "emotion", "score", fmt=".1%")

# ---- Labels ----
with tabs[5]:
    lab = module_result(status, "labels")
    if not lab:
        st.info("Pending")
    else:
        selected_labels = lab.get("labels") or []
        st.markdown("**Assigned labels:** " + (" ".join(f"`{x}`" for x in selected_labels) or "—"))
        df = scores_df(lab.get("scores"))
        if not df.empty:
            df["assigned"] = df["label"].isin(selected_labels)
            bars = alt.Chart(df).mark_bar().encode(
                x=alt.X("score:Q", title=None), y=alt.Y("label:N", sort="-x", title=None),
                color=alt.Color("assigned:N", scale=alt.Scale(domain=[True, False], range=["#4c78a8", "#c7c7c7"]), legend=None),
                tooltip=["label", alt.Tooltip("score:Q", format=".1%")],
            ).properties(height=max(120, 26 * len(df)))
            layers = [bars]
            if lab.get("threshold") is not None:
                layers.append(alt.Chart(pd.DataFrame({"t": [lab["threshold"]]})).mark_rule(color="#d62728", strokeDash=[4, 4]).encode(x="t:Q"))
            st.altair_chart(alt.layer(*layers), width="stretch")
            if lab.get("threshold") is not None:
                st.caption(f"Red line = assignment threshold ({lab['threshold']:.2f}).")

# Topics / keywords note
st.caption("Topics and keywords are corpus-level results: see the Topics & Labels page.")