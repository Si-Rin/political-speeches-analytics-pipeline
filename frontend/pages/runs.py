"""
Runs page: every collection run, its status, the discovery log (why each URL was or was not collected) and cancel.
Uses endpoints that already exist: GET /runs, GET /runs/{id}, GET /runs/{id}/log, POST /runs/{id}/cancel.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import streamlit as st
import altair as alt

import api_client

st.set_page_config(layout="wide")
st.title("Runs")

OUTCOMES = {  # CandidateStatus -> (label, colour, meaning)
    "ingested": ("Ingested", "#2ca02c", "Downloaded and stored in Bronze"),
    "duplicate_content": ("Duplicate content", "#9e9e9e", "Same file content already in Bronze"),
    "known_url_skipped": ("Already collected", "#9e9e9e", "URL already ingested earlier, skipped before download"),
    "prefilter_passed": ("Would be collected (dry run)", "#4c78a8", "Passed the filters; nothing was downloaded"),
    "prefilter_rejected": ("Filtered out", "#f2cf5b", "Rejected by URL / date / keyword filters"),
    "content_rejected": ("Content rejected", "#f58518", "Downloaded but unusable (empty, too large, no keyword match...)"),
    "fetch_failed": ("Download failed", "#d62728", "Network, HTTP, robots or format error"),
    "ingest_failed": ("Storage failed", "#d62728", "MinIO upload or database error"),
}
REASONS = {  # ReasonCode -> meaning
    "not_a_document_link": "Link does not point to a speech/document",
    "url_pattern_excluded": "URL matches an exclusion pattern",
    "out_of_date_range": "Outside the requested date range",
    "robots_disallowed": "Blocked by the site's robots.txt",
    "http_error": "HTTP error from the site",
    "timeout": "Request timed out",
    "content_too_large": "File larger than the allowed maximum",
    "empty_text": "No text could be extracted",
    "no_keyword_match": "None of the keywords found",
    "extraction_failed": "Text extraction failed",
    "checksum_exists": "Identical file already in Bronze",
    "upload_failed": "Upload to object storage failed",
    "db_error": "Database error",
    "budget_exhausted": "Download budget (max documents/pages) reached",
    "doc_type_excluded": "Document type not wanted",
    "access_restricted": "Access restricted (login / paywall)",
    "format_unavailable": "Requested format not available (e.g. YouTube)",
}

ACTIVE = ("queued", "running")
RETRYABLE = ("failed", "cancelled", "completed_with_errors")
PER_DOC_GOLD_MODULES = 6          # lexical, syntactic, entities, sentiment, emotions, labels (topics/keywords are corpus-level)
SELECT_KEY, OPEN_DOC_KEY = "runs_selected", "documents_selected_doc"
REFRESH_SECONDS = 5


def render_outcomes(counts: dict, reasons: dict):
    """What happened to every candidate discovered by the run"""
    if not counts:
        st.caption("No candidates were discovered by this run.")
        return
    total = sum(counts.values())
    st.subheader("Outcomes")
    if set(counts) == {"known_url_skipped"}:
        st.info(f"Nothing new to collect: all {total} items found were already in the database, so no download was needed.")
    df = pd.DataFrame([{"Outcome": OUTCOMES.get(k, (k, "#9e9e9e", ""))[0], "Candidates": v,
                        "Share": v / total, "Description": OUTCOMES.get(k, (k, "", ""))[2]} for k, v in counts.items()]
                      ).sort_values("Candidates", ascending=False)
    domain = [OUTCOMES.get(k, (k, "#9e9e9e", ""))[0] for k in counts]
    colors = [OUTCOMES.get(k, (k, "#9e9e9e", ""))[1] for k in counts]
    metrics = st.columns(min(len(df), 4))
    for i, row in enumerate(df.itertuples()):
        metrics[i % len(metrics)].metric(row.Outcome, row.Candidates, f"{row.Share:.0%} of {total}", delta_color="off")
    left, right = st.columns([3, 2])
    with left:
        st.altair_chart(
            alt.Chart(df).mark_bar().encode(
                y=alt.Y("Outcome:N", sort="-x", title=None), x=alt.X("Candidates:Q", title=None),
                color=alt.Color("Outcome:N", legend=None, scale=alt.Scale(domain=domain, range=colors)),
                tooltip=["Outcome", "Candidates", alt.Tooltip("Share:Q", format=".0%"), "Description"],
            ).properties(height=max(120, 34 * len(df))),
            width="stretch",
        )
    with right:
        st.dataframe(df[["Outcome", "Candidates", "Description"]], hide_index=True, width="stretch")
    if reasons:
        with st.expander(f"Why candidates were not collected ({sum(reasons.values())})", expanded=True):
            rdf = pd.DataFrame([{"Reason": REASONS.get(k, k), "Code": k, "Candidates": v} for k, v in reasons.items()]
                               ).sort_values("Candidates", ascending=False)
            st.dataframe(rdf, hide_index=True, width="stretch")


def _ts(value):
    return pd.to_datetime(value, utc=True) if value else None


def duration_seconds(detail: dict):
    start, end = _ts(detail.get("started_at")), _ts(detail.get("finished_at"))
    if start is None:
        return None
    return ((end or pd.Timestamp.now(tz="UTC")) - start).total_seconds()


def fmt_duration(seconds):
    """Format a duration to human readable form"""
    if seconds is None:
        return "—"
    seconds = int(seconds)
    return f"{seconds // 3600}h {seconds % 3600 // 60:02d}m" if seconds >= 3600 else (f"{seconds // 60}m {seconds % 60:02d}s" if seconds >= 60 else f"{seconds}s")


def fmt_time(value):
    ts = _ts(value)
    return ts.strftime("%Y-%m-%d %H:%M:%S") if ts is not None else "—"


def load_chain(run_id: str) -> list:
    """Documents created by this run with their Silver / Gold state (the run log links doc_id, the status API gives the states)."""
    log = api_client.get_run_log(run_id, status="ingested", limit=1000)
    items = [i for i in (log or {}).get("items", []) if i.get("doc_id")]
    if not items:
        return []
    statuses = {s["doc_id"]: s for s in (api_client.get_all_statuses() or [])}
    rows = []
    for item in items:
        st_ = statuses.get(item["doc_id"])
        silver = st_["silver_status"] if st_ else "not_started"
        gold_done = sum(1 for m in (st_ or {}).get("gold_modules", []) if m["done"])
        if silver == "failed":
            chain = "❌ Silver failed"
        elif silver == "success" and gold_done >= PER_DOC_GOLD_MODULES:
            chain = "✅ complete"
        elif silver == "success":
            chain = f"🟡 Gold {gold_done}/{PER_DOC_GOLD_MODULES}"
        else:
            chain = "⏳ processing"
        rows.append({"doc_id": item["doc_id"], "title": (item.get("title_hint") or item["source_url"])[:80],
                     "Bronze": "✅", "Silver": {"success": "✅", "failed": "❌"}.get(silver, "⏳"),
                     "Gold": f"{gold_done}/{PER_DOC_GOLD_MODULES}", "Chain": chain, "_chain_done": chain.startswith("✅"),
                     "_chain_failed": silver == "failed"})
    return rows


col1, col2 = st.columns([3, 1])
status_filter = col1.selectbox("Status", ["All", "queued", "running", "completed", "completed_with_errors", "failed", "cancelled"])
col2.write("")
col2.write("")
if col2.button("Refresh"):
    st.rerun()

runs = api_client.list_runs(limit=100, status=None if status_filter == "All" else status_filter)
if not runs:
    st.info("No runs yet. Start one from the Collection page.")
    st.stop()

st.dataframe(pd.DataFrame(runs).rename(columns={"documents_ingested": "ingested"}), width="stretch", hide_index=True)

run_ids = [r["run_id"] for r in runs]
if st.session_state.get(SELECT_KEY) not in run_ids:
    st.session_state.pop(SELECT_KEY, None)
run_id = st.selectbox("Run", run_ids, key=SELECT_KEY, format_func=lambda i: next(f"{r['source_name']} · {r['status']} · {str(r['requested_at'])[:19]} · {i[:8]}" for r in runs if r["run_id"] == i))

# Decide ONCE per full rerun whether the panel must keep refreshing: run in progress, or its documents are still being
# processed by Silver / Gold shortly after it finished (the Bronze flow chains them). Stops by itself otherwise.
first = api_client.get_run(run_id)
if not first:
    st.stop()
first_chain = load_chain(run_id) if first["status"] not in ACTIVE else []
recent = first.get("finished_at") and (pd.Timestamp.now(tz="UTC") - _ts(first["finished_at"])).total_seconds() < 3600
still_processing = bool(recent) and any(not r["_chain_done"] and not r["_chain_failed"] for r in first_chain)
keep_refreshing = first["status"] in ACTIVE or still_processing

@st.fragment(run_every=f"{REFRESH_SECONDS}s" if keep_refreshing else None)
def run_panel():
    detail = api_client.get_run(run_id)
    if not detail:
        return
    chain = load_chain(run_id)
    still = (detail["status"] in ACTIVE) or (detail["status"] != "queued" and any(not r["_chain_done"] and not r["_chain_failed"] for r in chain) and bool(detail.get("finished_at")))
    if keep_refreshing and not still:
        st.rerun()                      # everything settled: leave the fragment so the timer stops and the table above refreshes

    # ---- header: status, timing, speed ----
    seconds = duration_seconds(detail)
    ingested = detail.get("documents_ingested", 0)
    m = st.columns(5)
    m[0].metric("Status", ("🔄 " if detail["status"] in ACTIVE else "") + detail["status"].replace("_", " "))
    m[1].metric("Documents ingested", ingested)
    m[2].metric("Duration", fmt_duration(seconds))
    m[3].metric("Average speed", f"{seconds / ingested:.0f} s / document" if seconds and ingested else "—")
    m[4].metric("Mode", "dry run" if detail["dry_run"] else detail["mode"])
    t = st.columns(3)
    t[0].caption(f"Requested: {fmt_time(detail.get('requested_at'))}")
    t[1].caption(f"Started: {fmt_time(detail.get('started_at'))}")
    t[2].caption(f"Finished: {fmt_time(detail.get('finished_at')) if detail.get('finished_at') else ('in progress…' if detail['status'] in ACTIVE else '—')}")
    if detail["status"] in ACTIVE:
        st.caption(f"Refreshing every {REFRESH_SECONDS} seconds.")
    if detail.get("error_message"):
        st.error(detail["error_message"])

    # ---- actions ----
    if detail["status"] in ACTIVE and st.button("Cancel run", type="secondary", key="cancel_run"):
        if api_client.cancel_run(run_id):
            st.rerun()
    if detail["status"] in RETRYABLE and st.button("🔁 Relaunch with the same parameters", key="retry_run"):
        new = api_client.retry_run(run_id)
        if new:
            st.session_state["_pending_run_select"] = new["run_id"]
            st.rerun()

    render_outcomes(detail.get("counts") or {}, detail.get("reasons") or {})

    # ---- documents created by this run: did the chain Bronze -> Silver -> Gold work? ----
    if chain:
        st.subheader("Documents created by this run")
        done = sum(1 for r in chain if r["_chain_done"])
        failed = sum(1 for r in chain if r["_chain_failed"])
        st.caption(f"{done} of {len(chain)} fully processed (Bronze → Silver → Gold)" + (f" · {failed} failed in Silver" if failed else ""))
        st.dataframe(pd.DataFrame(chain).drop(columns=["_chain_done", "_chain_failed"]), hide_index=True, width="stretch")
        c1, c2 = st.columns([3, 1])
        doc = c1.selectbox("Document", [r["doc_id"] for r in chain], key="run_open_doc",
                           format_func=lambda i: next(f"#{i} — {r['title']}" for r in chain if r["doc_id"] == i))
        c2.write("")
        c2.write("")
        if c2.button("Open in Documents", key="open_doc"):
            st.session_state[OPEN_DOC_KEY] = doc
            st.session_state["documents_view"] = "Document"  # opens a preview  of the document
            st.switch_page("pages/documents.py")

    # ---- discovery log ----
    st.subheader("Discovery log")
    f1, f2 = st.columns(2)
    cand_status = f1.selectbox("Candidate status", ["All"] + list((detail.get("counts") or {}).keys()), key="log_status")
    reason = f2.selectbox("Reason", ["All"] + list((detail.get("reasons") or {}).keys()), key="log_reason")
    log = api_client.get_run_log(run_id, status=None if cand_status == "All" else cand_status,
                                 reason_code=None if reason == "All" else reason, limit=500)
    if log and log["items"]:
        cols = ["status", "reason_code", "title_hint", "source_url", "detail", "doc_id"]
        df = pd.DataFrame(log["items"])
        st.dataframe(df[[c for c in cols if c in df.columns]], width="stretch", hide_index=True,
                     column_config={"doc_id": st.column_config.NumberColumn("Document ID", format="%d")})
        st.caption(f"{log['total']} entries. Use “Open in Documents” above to see a created document's results.")
    else:
        st.caption("No log entries.")


run_panel()
