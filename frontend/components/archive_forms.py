"""
Archive-mode forms. `web_crawl` is the only archive source the backend runs today (phases 4a-4d add the
research sources: Miller Center, UCSB, YouTube collection, IA collection); this stays "Advanced" until then.
The fuller archive experience (research keywords everywhere, discovery-log preview, cost hint) is phase 5.
"""
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


RENDERERS = {
    "Web crawl (Advanced)": render_web_crawl,
}
