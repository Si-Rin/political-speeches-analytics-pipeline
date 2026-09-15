"""
Routes: Collection (probe/submit) and History, HTTP layer only.

 — probing logic lives in pipeline_probe.py
 — triggering logic in pipeline_runner.py
 — DB access is plain psycopg2 via prefect_flows.clients (safe to import — see note in pipeline_runner.py: clients.py has no prefect dependency).
"""
from typing import Optional

from fastapi import APIRouter, HTTPException

from prefect_flows.clients import get_postgres_connection

from backend.schemas import (
    CrawlRequest, CrawlResponse, ProbeRequest, ProbeResponse,
    SubmitRequest, SubmitResponse,
    DocumentSummary, DocumentHistoryResponse,
)
from backend.pipeline_probe import probe, classify_source
from backend.pipeline_runner import trigger_crawl, trigger_single_item_ingestion

router = APIRouter()

# This project is dedicated exclusively to Donald Trump speeches.
# The client never supplies a speaker value; the backend owns this invariant.
SPEAKER = "Donald Trump"


@router.post("/documents/probe", response_model=ProbeResponse)
def probe_source(req: ProbeRequest):
    """
    Lightweight metadata detection — no download, no DB write.
    Used by the Collection UI's "review detected information" step, before the user confirms with /documents/submit.
    """
    result = probe(req.location, req.is_local, req.content_type)
    return ProbeResponse(**result)


@router.post("/documents/submit", response_model=SubmitResponse)
def submit_source(req: SubmitRequest):
    """
    Triggers Bronze ingestion for one item and returns immediately.
    Does not wait for it to finish (that can take a while for video/audio downloads).

    The speaker is deliberately not part of SubmitRequest: this API owns the project-level invariant that every collected speech belongs to Donald Trump.
    """

    source_kind = classify_source(req.location, req.is_local)

    raw_metadata = dict(req.raw_metadata or {})
    user_provided = {
        k: v for k, v in {
            "title": req.title,
            "publication_date": req.publication_date.isoformat() if req.publication_date else None,
            "language": req.language,
            "notes": req.notes,
        }.items() if v is not None
    }
    if user_provided:
        raw_metadata["user_provided"] = user_provided

    trigger_single_item_ingestion(
        location=req.location,
        is_local=req.is_local,
        content_type=req.content_type,
        source_kind=source_kind,
        raw_metadata=raw_metadata or None,
    )
    return SubmitResponse(
        accepted=True,
        message="Ingestion started. Check History shortly — it can take a "
                "moment for the document to appear (longer for video/audio).",
        location=req.location,
    )


@router.post("/documents/crawl", response_model=CrawlResponse)
def crawl_sources(req: CrawlRequest):
    """
    Triggers a keyword-based web crawl (WebCrawlSource) — discovers and ingests every page that clears the keyword relevance bar, starting from seed_urls.
    Distinct from /documents/submit: this can surface many documents over several minutes, not one.
    See pipeline_runner.py for why WebCrawlSource/UcsbTweetsSource-style crawlers were kept separate from the single-item routing in classify_source().
    """
    trigger_crawl(
        seed_urls=req.seed_urls,
        keywords=req.keywords,
        allowed_domains=req.allowed_domains,
        max_depth=req.max_depth if req.max_depth is not None else 2,
        max_pages=req.max_pages if req.max_pages is not None else 50,
    )
    return CrawlResponse(
        accepted=True,
        message="Crawl started — this can take a few minutes. New "
                "documents will appear in History as they're found.",
    )


@router.get("/documents/history", response_model=DocumentHistoryResponse)
def get_history(limit: int = 100, source_type: Optional[str] = None):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            query = """
                SELECT b.doc_id, b.source_url, b.file_name, b.source_type,
                       b.ingestion_date,
                       s.title, s.speaker, s.publication_date, s.status_processing
                FROM bronze.documents b
                LEFT JOIN silver.text s ON s.doc_id = b.doc_id
                WHERE b.excluded = FALSE
            """
            params = []
            if source_type:
                query += " AND b.source_type = %s"
                params.append(source_type)
            query += " ORDER BY b.ingestion_date DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()
    finally:
        conn.close()

    documents = []
    for r in rows:
        (doc_id, source_url, file_name, source_type_,
         ingestion_date, title, speaker, publication_date, status_processing) = r
        documents.append(DocumentSummary(
            doc_id=doc_id,
            source=source_url or file_name or "",
            source_type=source_type_,
            speaker=speaker,
            title=title,
            publication_date=publication_date,
            ingestion_date=ingestion_date,
            silver_status=status_processing or "not_started",
        ))

    return DocumentHistoryResponse(documents=documents)


@router.get("/documents/{doc_id}", response_model=DocumentSummary)
def get_document(doc_id: int):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT b.doc_id, b.source_url, b.file_name, b.source_type,
                       b.ingestion_date,
                       s.title, s.speaker, s.publication_date, s.status_processing
                FROM bronze.documents b
                LEFT JOIN silver.text s ON s.doc_id = b.doc_id
                WHERE b.doc_id = %s AND b.excluded = FALSE
                """,
                (doc_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if row is None:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")

    (doc_id, source_url, file_name, source_type_,
     ingestion_date, title, speaker, publication_date, status_processing) = row
    return DocumentSummary(
        doc_id=doc_id,
        source=source_url or file_name or "",
        source_type=source_type_,
        speaker=speaker,
        title=title,
        publication_date=publication_date,
        ingestion_date=ingestion_date,
        silver_status=status_processing or "not_started",
    )