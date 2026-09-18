"""
Sentiment & Entities page: sentiment/emotion distributions and the most frequent named entities across the corpus.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st

from api_client import get_sentiment, get_emotions, get_entities

st.set_page_config(layout="wide")
st.title("Sentiment & Entities")

if st.button("Refresh"):
    st.rerun()

sentiment_data = get_sentiment()
emotions_data = get_emotions()

col1, col2 = st.columns(2)

with col1:
    st.subheader("Sentiment distribution")
    if sentiment_data and sentiment_data.get("distribution"):
        df = pd.DataFrame(sentiment_data["distribution"])
        st.bar_chart(df.set_index("label")["count"])
    else:
        st.info("No sentiment data available yet.")

with col2:
    st.subheader("Emotion distribution")
    if emotions_data and emotions_data.get("distribution"):
        df = pd.DataFrame(emotions_data["distribution"])
        st.bar_chart(df.set_index("emotion")["count"])
    else:
        st.info("No emotion data available yet.")

if sentiment_data and sentiment_data.get("over_time"):
    st.divider()
    st.subheader("Sentiment over time")
    df_time = pd.DataFrame(sentiment_data["over_time"]).set_index("month")
    st.line_chart(df_time[["positive", "negative", "neutral"]])

st.divider()
st.subheader("Most frequent entities")

top_n = st.slider("Top N per category", min_value=5, max_value=30, value=15)
entities_data = get_entities(top_n=top_n)

if not entities_data:
    st.info("No entity data available yet.")
else:
    col_a, col_b, col_c = st.columns(3)
    with col_a:
        st.markdown("**Persons**")
        df = pd.DataFrame(entities_data.get("persons", []))
        if not df.empty:
            st.dataframe(df, width='stretch', hide_index=True)
    with col_b:
        st.markdown("**Locations**")
        df = pd.DataFrame(entities_data.get("locations", []))
        if not df.empty:
            st.dataframe(df, width='stretch', hide_index=True)
    with col_c:
        st.markdown("**Groups**")
        df = pd.DataFrame(entities_data.get("groups", []))
        if not df.empty:
            st.dataframe(df, width='stretch', hide_index=True)