"""
Collection page: add a source -> review detected information -> submit -> see confirmation.


- "Submit a document": add a source -> review detected information -> submit -> see confirmation. One document at a time.
- "Crawl for documents": keyword-based web crawl (WebCrawlSource) that discovers and ingests several matching pages at once, starting from seed URLs.
   No review step here — a crawl doesn't resolve to a single set of "detected info" the way one document does.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import streamlit as st

from api_client import probe_source, submit_source

st.set_page_config(layout="wide")

st.title("Add a source")

mode = st.radio("Mode", ["Submit a document", "Crawl for documents"], horizontal=True)
st.divider()

# ---------------------------------------------------------------------------
# Mode: Submit a document
# ---------------------------------------------------------------------------
if mode == "Submit a document":
    if "probe_result" not in st.session_state:
        st.session_state.probe_result = None
    if "probed_for" not in st.session_state:
        st.session_state.probed_for = None
 
    with st.form("add_source_form"):
        source_kind = st.radio("Source", ["URL", "Local path"], horizontal=True)
        location = st.text_input(
            "Location",
            placeholder="https://..." if source_kind == "URL" else "/path/to/file.mp4",
        )
        content_type = st.radio("Content type", ["text", "video", "audio"], horizontal=True)
        detect_clicked = st.form_submit_button("Detect")
 
    if detect_clicked:
        if not location.strip():
            st.warning("Enter a URL or local path first.")
        else:
            is_local = source_kind == "Local path"
            with st.spinner("Detecting..."):
                result = probe_source(location.strip(), is_local, content_type)
            st.session_state.probe_result = result
            st.session_state.probed_for = {
                "location": location.strip(), "is_local": is_local, "content_type": content_type,
            }
 
    result = st.session_state.probe_result
    probed_for = st.session_state.probed_for
 
    if result is not None:
        st.subheader("Detected information")
        if not result.get("ok"):
            for w in result.get("warnings", []):
                st.error(w)
        else:
            cols = st.columns(3)
            cols[0].metric("Title", result.get("detected_title") or "—")
            cols[1].metric("File name", result.get("detected_file_name") or "—")
            cols[2].metric("MIME type", result.get("detected_mime_type") or "—")
 
            cols2 = st.columns(3)
            size = result.get("detected_size_bytes")
            cols2[0].metric("Size", f"{size / 1_000_000:.1f} MB" if size else "—")
            duration = result.get("detected_duration_seconds")
            cols2[1].metric("Duration", f"{duration / 60:.1f} min" if duration else "—")
            cols2[2].metric("Uploader", result.get("detected_uploader") or "—")
 
            for w in result.get("warnings", []):
                st.warning(w)
 
            if st.button("Confirm & submit", type="primary"):
                with st.spinner("Starting ingestion..."):
                    submit_result = submit_source(
                        probed_for["location"],
                        probed_for["is_local"],
                        probed_for["content_type"],
                        result.get("raw_metadata"),
                    )
                if submit_result and submit_result.get("accepted"):
                    st.success(submit_result["message"])
                    st.session_state.probe_result = None
                    st.session_state.probed_for = None
                else:
                    st.error("Submission failed.")
 
    st.divider()
    st.subheader("Available sources")
    st.markdown("""
Paste a URL or local path above — the right source is picked automatically,
no need to select it:
 
| Source | How it's detected | Notes |
|---|---|---|
| **YouTube** | `youtube.com` / `youtu.be` links | Title, duration, uploader pulled via yt-dlp |
| **Miller Center speeches** | `millercenter.org` links | Always ingested as text — the content type above is ignored for this source |
| **Internet Archive (TV News)** | `archive.org` links | Always ingested as audio — the content type above is ignored for this source |
| **Direct file link** | any other URL | Uses the content type selected above |
| **Local file** | a path on this machine | Uses the content type selected above |
 
Need to discover several documents at once instead of one? Switch to
**Crawl for documents** above. *Not available from either mode yet —
command-line only:* syncing the UCSB tweets archive.
""")
 
# ---------------------------------------------------------------------------
# Mode: Crawl for documents
# ---------------------------------------------------------------------------
else:
    st.caption(
        "Starts from your seed URL(s) and follows links outward, keeping "
        "only pages that match your keywords. Can take a few minutes and "
        "surface several documents — no single \"detected info\" review "
        "step here. Watch History for new rows as they're found."
    )
 
    with st.form("crawl_form"):
        seed_urls_raw = st.text_area(
            "Seed URL(s)", placeholder="https://example.com/speeches\nhttps://example.com/press-releases",
            help="One per line. The crawl starts here and follows links outward.",
        )
        keywords_raw = st.text_area(
            "Keywords", placeholder="rally\nspeech\npress conference",
            help="One per line. A page is only kept if it matches enough of these (see Keyword threshold).",
        )
        allowed_domains_raw = st.text_input(
            "Allowed domains (optional)", placeholder="example.com, example.org",
            help="Comma-separated. Recommended — without this the crawl can wander off-site.",
        )
        col1, col2 = st.columns(2)
        max_depth = col1.number_input("Max depth", min_value=1, value=2, step=1)
        max_pages = col2.number_input("Max pages", min_value=1, value=50, step=1)
 
        crawl_clicked = st.form_submit_button("Start crawl", type="primary")
 
    if crawl_clicked:
        seed_urls = [u.strip() for u in seed_urls_raw.splitlines() if u.strip()]
        keywords = [k.strip() for k in keywords_raw.splitlines() if k.strip()]
        allowed_domains = [d.strip() for d in allowed_domains_raw.split(",") if d.strip()] or None
 
        if not seed_urls or not keywords:
            st.warning("At least one seed URL and one keyword are required.")
        else:
            with st.spinner("Starting crawl..."):
                crawl_result = trigger_crawl(
                    seed_urls=seed_urls, keywords=keywords,
                    allowed_domains=allowed_domains,
                    max_depth=int(max_depth), max_pages=int(max_pages),
                )
            if crawl_result and crawl_result.get("accepted"):
                st.success(crawl_result["message"])
            else:
                st.error("Could not start the crawl.")
 