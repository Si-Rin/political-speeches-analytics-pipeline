"""
Landing page. 

Streamlit auto-discovers frontend/pages/*.py and lists them in the sidebar next to this one 
No custom routing code needed.

Run with: streamlit run frontend/app.py
"""
import streamlit as st

from api_client import API_BASE_URL

st.set_page_config(page_title="Political Speeches Analytics", layout="wide")

st.title("Political Speeches Analytics")
st.write(
    "Use the sidebar to add a new source, review previously collected documents, or check pipeline processing status."
)
st.caption(f"Backend API: {API_BASE_URL}")