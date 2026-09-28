"""
Routes: probe (lightweight metadata detection) and History. HTTP layer only.

Collecting is done through /runs (routes/runs.py); probing logic lives in pipeline_probe.py.
"""
from typing import Optional

from fastapi import APIRouter, HTTPException

from backend.db import get_connection
from backend.schemas import (
    ProbeRequest, ProbeResponse, DocumentSummary, DocumentHistoryResponse,
)
from backend.pipeline_probe import probe

router = APIRouter()


@router.post("/documents/probe", response_model=ProbeResponse)
def probe_source(req: ProbeRequest):
    """
    Lightweight metadata detection — no download, no DB write.
    Used by the Collection UI's "review detected information" step, before the user starts a run with POST /runs.
    """
    result = probe(req.location, req.is_local, req.content_type)
    return ProbeResponse(**result)


@router.get("/documents/history", response_model=DocumentHistoryResponse)
def get_history(source_type: Optional[str] = None):
    conn = get_connection()
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
            query += " ORDER BY b.ingestion_date DESC"

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
    conn = get_connection()
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