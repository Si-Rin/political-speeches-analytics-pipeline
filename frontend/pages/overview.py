"""
Analytics Overview page: corpus-level statistics.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import API_BASE_URL, get_corpus_summary, get_emotions, get_entities, get_overview, get_sentiment, get_topics

import altair as alt
from components.charts import donut, hbar

st.set_page_config(layout="wide")
st.title("Analytics Overview")
st.caption(f"Collect political speeches, process them through Bronze → Silver → Gold, and explore the corpus.")

overview = get_overview()
corpus = get_corpus_summary(top_n=15)
sentiment = get_sentiment()
emotions = get_emotions()
entities = get_entities(top_n=10)
topics = get_topics()

if not overview or not overview.get("total_documents"):
    st.info("No documents collected yet. Start from the Collection page.")
    st.stop()

if st.button("Refresh"):
    st.rerun()


def top_of(distribution, key):
    """(label, pct) of the most frequent category of a {label,count,pct} distribution, or (None, None)."""
    if not distribution:
        return None, None
    best = max(distribution, key=lambda d: d["count"])
    return best[key], best["pct"]


st.markdown("""
<style>
/* Allow long metric values to wrap instead of ending with ... */
[data-testid="stMetricValue"],
[data-testid="stMetricValue"] div {
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: clip !important;
    overflow-wrap: anywhere !important;
    word-break: normal !important;
    max-width: 100% !important;
    line-height: 1.2 !important;
}

/* Allow metric labels to wrap too */
[data-testid="stMetricLabel"],
[data-testid="stMetricLabel"] p,
[data-testid="stMetricLabel"] div {
    white-space: normal !important;
    overflow: visible !important;
    text-overflow: unset !important;
}
</style>
""", unsafe_allow_html=True)


# ---------------------------------------------------------------------------
# 1. Corpus at a glance
# ---------------------------------------------------------------------------
st.subheader("Corpus at a glance")
analysed = (corpus or {}).get("documents_analyzed", 0)
k = st.columns(5)
k[0].metric("Documents", overview["total_documents"])
k[1].metric("Silver processed", overview["silver_success"], help=f"{overview['silver_failed']} failed")
k[2].metric("Fully or partly analysed", analysed)
k[3].metric("Total words", f"{(corpus or {}).get('total_words', 0):,}")
date_range = f"{overview['date_min']} → {overview['date_max']}" if overview.get("date_min") else "—"
k[4].metric("Date range", date_range)

# ---------------------------------------------------------------------------
# 2. Headline findings (what the analysis says about the whole corpus)
# ---------------------------------------------------------------------------
st.subheader("Headline findings")
dom_sent, dom_sent_pct = top_of((sentiment or {}).get("distribution"), "label")
dom_emo, dom_emo_pct = top_of((emotions or {}).get("distribution"), "emotion")
top_word = ((corpus or {}).get("top_words") or [{}])[0]
top_person = ((entities or {}).get("persons") or [{}])[0]
top_place = ((entities or {}).get("locations") or [{}])[0]
top_label = ((corpus or {}).get("label_distribution") or [{}])[0]

h = st.columns(6)
h[0].metric("Dominant sentiment", (dom_sent or "—").capitalize(),
            f"{dom_sent_pct}% of documents" if dom_sent else None, delta_color="off")
h[1].metric("Dominant emotion", (dom_emo or "—").capitalize(),
            f"{dom_emo_pct}% of documents" if dom_emo else None, delta_color="off")
h[2].metric("Top keyword", top_word.get("word", "—"), f"{top_word['count']:,} uses" if top_word else None, delta_color="off")
h[3].metric("Most cited person", top_person.get("text", "—"), f"{top_person['count']} mentions" if top_person else None, delta_color="off")
h[4].metric("Most cited place", top_place.get("text", "—"), f"{top_place['count']} mentions" if top_place else None, delta_color="off")
h[5].metric("Main theme", top_label.get("label", "—"), f"{top_label['pct']}% of documents" if top_label else None, delta_color="off")

if not corpus or not analysed:
    st.info("No Gold analysis yet: statistics below appear once documents have gone through Silver and Gold.")

# ---------------------------------------------------------------------------
# 3. Tone (sentiment & emotions)
# ---------------------------------------------------------------------------
if corpus and analysed:
    st.divider()
    st.subheader("Tone of the corpus")
    sentiment_dist, emotions_dist, emo_intensity_dist = st.columns(3)
    with sentiment_dist:
        st.markdown("**Sentiment (documents)**")
        donut(pd.DataFrame((sentiment or {}).get("distribution") or []), "label", "count")
    with emotions_dist:
        st.markdown("**Emotions (dominant, documents)**")
        hbar(pd.DataFrame((emotions or {}).get("distribution") or []), "emotion", "count", fmt=",d")
    with emo_intensity_dist:
        st.markdown("**Average emotion intensity**")
        hbar(pd.DataFrame([{"emotion": e, "mean score": v} for e, v in (corpus["emotion_scores"] or {}).items()]), "emotion", "mean score", fmt=".1%", color="#e45756")
    if (sentiment or {}).get("over_time"):
        st.markdown("**Sentiment over time**")
        df_time = pd.DataFrame(sentiment["over_time"]).set_index("month")
        st.line_chart(df_time[["positive", "negative", "neutral"]])

    # -----------------------------------------------------------------------
    # 4. Keywords & phrases
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("Keywords and phrases")
    top_kws, n_grams = st.columns(2)
    with top_kws:
        st.markdown("**Top keywords** (most frequent content words)")
        hbar(pd.DataFrame(corpus["top_words"]), "word", "count", fmt=",d")
    with n_grams:
        bigrams, trigrams, anaphora = st.tabs(["Bigrams", "Trigrams", "Repeated openings"])
        for tab, key in ((bigrams, "top_bigrams"), (trigrams, "top_trigrams"), (anaphora, "top_repetitions")):
            with tab:
                if corpus[key]:
                    st.dataframe(pd.DataFrame(corpus[key]), hide_index=True, width="stretch")
                else:
                    st.caption("None found.")

    # -----------------------------------------------------------------------
    # 5. Entities
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("Most mentioned entities")
    cols = st.columns(3)
    for col, key, title in zip(cols, ("persons", "locations", "groups"), ("Persons", "Locations", "Groups")):
        with col:
            st.markdown(f"**{title}**")
            hbar(pd.DataFrame((entities or {}).get(key) or []), "text", "count", fmt=",d")

    # -----------------------------------------------------------------------
    # 6. Rhetoric & style
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("Rhetoric and style")
    pronouns, oppositions = st.columns(2)
    with pronouns:
        st.markdown("**Pronoun use** (average per 1,000 words)")
        hbar(pd.DataFrame([{"pronoun": k_.replace("_", " "), "per 1000 words": v}
                           for k_, v in (corpus["pronoun_ratios"] or {}).items()]), "pronoun", "per 1000 words")
        m = st.columns(2)
        m[0].metric("Lexical diversity (TTR)", f"{corpus['avg_ttr']:.3f}" if corpus.get("avg_ttr") is not None else "—")
        m[1].metric("Avg sentence length", f"{corpus['avg_sentence_length']:.1f} words" if corpus.get("avg_sentence_length") else "—")
        st.caption(
            "TTR (Type‑Token Ratio) = nombre de mots uniques ÷ nombre total de mots. "
            "Plus la valeur est proche de 1, plus le vocabulaire est varié."
        )
    with oppositions:
        st.markdown("**Binary oppositions** (occurrences of each side)")
        opp = corpus["oppositions"]
        if opp:
            long = pd.DataFrame([{"pair": o["pair"], "word": w, "count": n} for o in opp for w, n in ((o["left"], o["left_count"]), (o["right"], o["right_count"]))])
            st.altair_chart(
                alt.Chart(long).mark_bar().encode(
                    y=alt.Y("pair:N", title=None), x=alt.X("count:Q", title=None), yOffset="word:N",
                    color=alt.Color("word:N", legend=None), tooltip=["word", "count"],
                ).properties(height=max(120, 46 * len(opp))),
                width="stretch",
            )
        else:
            st.caption("None found.")

    # -----------------------------------------------------------------------
    # 7. Themes
    # -----------------------------------------------------------------------
    st.divider()
    st.subheader("Themes")
    a, b = st.columns(2)
    with a:
        st.markdown("**Dominant label per document**")
        hbar(pd.DataFrame(corpus["label_distribution"]), "label", "count", fmt=",d")
    with b:
        st.markdown("**Largest BERTopic topics** (corpus-level, c-TF-IDF keywords)")
        rows = [{"Corpus": t["corpus"].replace("_", "-"), "Documents": t["n_docs"], "Keywords": ", ".join(t["keywords"][:6])}
                for t in sorted((topics or {}).get("topics") or [], key=lambda t: -t["n_docs"]) if not t["is_outlier"]][:8]
        if rows:
            st.dataframe(pd.DataFrame(rows), hide_index=True, width="stretch")
        else:
            st.caption("No topics computed yet. Run the topic modeling from the Topics page.")

# ---------------------------------------------------------------------------
# 8. Collection statistics
# ---------------------------------------------------------------------------
st.divider()
st.subheader("What was collected")
a, b = st.columns(2)
with a:
    st.markdown("**By content type**")
    df_type = pd.DataFrame(overview["by_source_type"])
    if not df_type.empty:
        donut(df_type, "source_type", "count")
with b:
    st.markdown("**By origin**")
    hbar(pd.DataFrame(overview["by_origin"]), "source_name", "count", fmt=",d")

if overview.get("word_count_stats"):
    wc = overview["word_count_stats"]
    st.markdown("**Document length (words)**")
    cols = st.columns(5)
    for col, label, key in zip(cols, ("Min", "Max", "Mean", "Median", "Std dev"), ("min", "max", "mean", "median", "stddev")):
        col.metric(label, f"{wc[key]:,.0f}")

if overview.get("documents_over_time"):
    st.markdown("**Documents over time**")
    st.bar_chart(pd.DataFrame(overview["documents_over_time"]).set_index("month")["count"])
