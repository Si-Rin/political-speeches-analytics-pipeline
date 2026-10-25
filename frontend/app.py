"""
Entry point / router only (no page of its own). 

The sidebar order is explicitly defined via st.navigation (default landing page : Overview).
Run with: streamlit run frontend/app.py
"""
import streamlit as st

from api_client import API_BASE_URL

st.set_page_config(page_title="Political Speeches Analytics", page_icon="🎙️", layout="wide")

pages = [
    st.Page("pages/overview.py", title="Overview", icon="📊", url_path="overview", default=True),
    st.Page("pages/collection.py", title="Collection", icon="📥", url_path="collection"),
    st.Page("pages/runs.py", title="Runs", icon="🏃", url_path="runs"),
    st.Page("pages/history.py", title="History", icon="🗂️", url_path="history"),
    st.Page("pages/pipeline_status.py", title="Pipeline Status", icon="⚙️", url_path="pipeline-status"),
    st.Page("pages/sentiment_emotions_entities.py", title="Sentiment & Entities", icon="💬", url_path="sentiment-entities"),
    st.Page("pages/topics_labels.py", title="Topics & Labels", icon="🏷️", url_path="topics-labels"),
]

st.navigation(pages).run()