"""Per-document Gold results (lexical, syntactic, entities, sentiment, emotions, labels) as charts and tables"""
import altair as alt
import pandas as pd
import streamlit as st

from components.charts import hbar

PER_DOC_MODULES = ["lexical", "syntactic", "entities", "sentiment", "emotions", "labels"]


def scores_df(scores: dict, label: str = "label") -> pd.DataFrame:
    return pd.DataFrame([{label: k, "score": v} for k, v in (scores or {}).items()]).sort_values("score", ascending=False) \
        if scores else pd.DataFrame(columns=[label, "score"])


def module_result(status: dict, name: str):
    for m in status["gold_modules"]:
        if m["module"] == name:
            return m.get("result")
    return None


def render_gold_tabs(status: dict) -> None:
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
                hbar(pd.DataFrame([{"pronoun": k.replace("_", " "), "per_1000": v}
                                   for k, v in (lex.get("pronoun_ratios") or {}).items()]), "pronoun", "per_1000")
            with b:
                st.markdown("**Most frequent content words**")
                hbar(pd.DataFrame(lex.get("top_content_words") or []), "word", "count", fmt=",d")

            opp = lex.get("oppositions") or []
            if opp:
                st.markdown("**Binary oppositions** (occurrences of each side)")
                long = pd.DataFrame([{"pair": f"{o['pair'][0]} vs {o['pair'][1]}", "word": w, "count": n}
                                     for o in opp for w, n in zip(o["pair"], o["counts"])])
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
                            tooltip=["tense", "percent"]).properties(height=260),
                        width="stretch",
                    )
            with c:
                st.markdown("**Modality** (per 1,000 words)")
                hbar(pd.DataFrame([{"modality": k.replace("_modality", ""), "per_1000": v}
                                   for k, v in (syn.get("modality") or {}).items()]), "modality", "per_1000")

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
