"""
Archive-mode forms. `web_crawl` is the only archive source the backend runs today (phases 4a-4d add the
research sources: Miller Center, UCSB, YouTube collection, IA collection); this stays "Advanced" until then.
The fuller archive experience (research keywords everywhere, discovery-log preview, cost hint) is phase 5.
"""
import re
from typing import Optional

import streamlit as st

import api_client


def render_web_crawl() -> Optional[dict]:
    st.caption(
        "Starts from your seed URL(s) and follows links outward, keeping only pages that match your keywords. "
        "Can take a few minutes and surface several documents — watch History for new rows as they're found."
    )
    with st.form("archive_web_crawl"):
        seed_urls_raw = st.text_area(
            "Seed URL(s)", placeholder="https://example.com/speeches\nhttps://example.com/press-releases",
            help="One per line. The crawl starts here and follows links outward.", key="crawl_seed_urls",
        )
        keywords_raw = st.text_area(
            "Keywords", placeholder="rally\nspeech\npress conference",
            help="One per line. A page is kept only if it matches these keywords.", key="crawl_keywords",
        )
        allowed_domains_raw = st.text_input(
            "Allowed domains (optional)", placeholder="example.com, example.org",
            help="Comma-separated. Recommended — without this the crawl can wander off-site.", key="crawl_domains",
        )
        col1, col2 = st.columns(2)
        max_depth = col1.number_input("Max depth", min_value=2, max_value=5, value=2, step=1, key="crawl_max_depth")
        max_pages = col2.number_input("Max pages (pages visited, not documents found)", min_value=1, max_value=500,
                                      value=50, step=1, key="crawl_max_pages")
        submitted = st.form_submit_button("Start crawl", type="primary")

    if not submitted:
        return None
    seed_urls = [u.strip() for u in seed_urls_raw.splitlines() if u.strip()]
    keywords = [k.strip() for k in keywords_raw.splitlines() if k.strip()]
    allowed_domains = [d.strip() for d in allowed_domains_raw.split(",") if d.strip()] or None

    if not seed_urls or not keywords:
        st.warning("At least one seed URL and one keyword are required.")
        return None

    with st.spinner("Starting crawl..."):
        run = api_client.create_run("web_crawl", seed_urls=seed_urls, keywords=keywords,
                                    allowed_domains=allowed_domains, max_depth=int(max_depth), max_pages=int(max_pages))
    if run and run.get("status") != "failed":
        st.success(f"Crawl started (run `{run['run_id']}`) — this can take a few minutes. New documents will appear in History as they're found.")
    return run


def render_miller_center() -> Optional[dict]:
    st.caption("Presidential speeches from millercenter.org: give speech page URLs, or the speeches listing page (filters in the URL, e.g. one president, are kept; all its pages are followed).")
    with st.form("archive_miller_center"):
        urls_raw = st.text_area("Speech page URL(s)", help="One per line (optional if a listing page is given).", key="mc_urls")
        start_url = st.text_input("Listing page (optional)", placeholder="https://millercenter.org/the-presidency/presidential-speeches", key="mc_start")
        max_depth = st.number_input("Max speeches to collect", min_value=1, max_value=1000, value=100, step=1, key="mc_depth")
        submitted = st.form_submit_button("Start collection", type="primary")
    if not submitted:
        return None
    urls = [u.strip() for u in urls_raw.splitlines() if u.strip()]
    if not urls and not start_url.strip():
        st.warning("Provide speech URL(s) or a listing page.")
        return None
    with st.spinner("Starting collection..."):
        run = api_client.create_run("miller_center", urls=urls or None, start_url=start_url.strip() or None, max_depth=int(max_depth))
    if run and run.get("status") != "failed":
        st.success(f"Collection started (run `{run['run_id']}`). Follow it on the Runs page.")
    return run


def ucsb_link_text_filter(url: str) -> Optional[str]:
    """Same rule as backend/schemas/sources.py: 'first-term' -> tweets, 'second-term' -> Truth Social."""
    normalized = re.sub(r"[\s_+]|%20", "-", url.lower())
    first, second = "first-term" in normalized, "second-term" in normalized
    if first == second:
        return None
    return "tweets of" if first else "truth social posts of"


def render_ucsb() -> Optional[dict]:
    st.caption("American Presidency Project (presidency.ucsb.edu): a search/listing URL.  paste the listing URL of the term to collect, either first or second term.")
    with st.form("archive_ucsb"):
        listing_url = st.text_input("Listing URL", placeholder="https://www.presidency.ucsb.edu/...", key="ucsb_url")
        link_text_filter = st.text_input("Link text filter", value="tweets of", key="ucsb_filter")
        max_documents = st.number_input("Max NEW documents", min_value=1, max_value=500, value=10, step=1, key="ucsb_max", help="Pages already collected are skipped and do not count toward this limit.")
        submitted = st.form_submit_button("Start collection", type="primary")
    if not submitted:
        return None
    if not listing_url.strip():
        st.warning("Enter a listing URL.")
        return None
    with st.spinner("Starting collection..."):
        run = api_client.create_run("ucsb", listing_url=listing_url.strip(), link_text_filter=link_text_filter.strip() or "tweets of",
                                    max_documents=int(max_documents))
    if run and run.get("status") != "failed":
        st.success(f"Collection started (run `{run['run_id']}`). Follow it on the Runs page.")
    return run


def render_youtube_playlist() -> Optional[dict]:
    st.caption("Download the videos of a YouTube playlist (or channel page), one after the other. "
               "Videos already collected are skipped. Audio only is much faster and enough for the text analysis.")
    with st.form("archive_youtube_playlist"):
        playlist_url = st.text_input("Playlist URL", placeholder="https://www.youtube.com/playlist?list=...", key="ytp_url")
        fmt = st.radio("Format", ["Audio only (recommended)", "Video"], horizontal=True, key="ytp_format")
        max_videos = st.number_input("Max videos", min_value=1, max_value=200, value=10, step=1, key="ytp_max",
                                     help="Taken from the start of the playlist.")
        submitted = st.form_submit_button("Start collection", type="primary")
    if not submitted:
        return None
    if not playlist_url.strip():
        st.warning("Enter a playlist URL.")
        return None
    with st.spinner("Starting collection..."):
        run = api_client.create_run("youtube_collection", playlist_url=playlist_url.strip(),
                                    download_format="audio" if fmt.startswith("Audio") else "video", max_videos=int(max_videos))
    if run and run.get("status") != "failed":
        st.success(f"Collection started (run `{run['run_id']}`). Follow it on the Runs page.")
    return run


RENDERERS = {
    "Miller Center": render_miller_center,
    "UCSB": render_ucsb,
    "YouTube playlist": render_youtube_playlist,
    "Web crawl (Advanced)": render_web_crawl,
}
