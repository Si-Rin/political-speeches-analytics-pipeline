"""
Routes: probe (lightweight metadata detection) and History. HTTP layer only.

Collecting is done through /runs (routes/runs.py); probing logic lives in pipeline_probe.py.
"""
from typing import Optional

from urllib.parse import quote

from fastapi import APIRouter, HTTPException
from fastapi.responses import PlainTextResponse, StreamingResponse

from backend import storage
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

    (doc_id, source_url, file_name, source_type_, ingestion_date, title, speaker, publication_date, status_processing) = row
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

@router.get("/documents/{doc_id}/download")
def download_document(doc_id: int):
    """Stream the raw Bronze file (the original video/audio/html/pdf) from MinIO."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT storage_path, file_name, mime_type FROM bronze.documents WHERE doc_id = %s AND excluded = FALSE",
                        (doc_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
    storage_path, file_name, mime_type = row
    try:
        obj = storage.open_bronze(storage_path)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="The raw file is missing from object storage.")
    except storage.StorageUnavailable as exc:
        raise HTTPException(status_code=503, detail=str(exc))

    def _chunks():
        try:
            yield from obj.stream(1024 * 1024)
        finally:
            obj.close()
            obj.release_conn()

    name = file_name or storage_path
    return StreamingResponse(_chunks(), media_type=mime_type or "application/octet-stream",
                             headers={"Content-Disposition": f"attachment; filename*=UTF-8''{quote(name)}"})


@router.get("/documents/{doc_id}/transcript", response_class=PlainTextResponse)
def download_transcript(doc_id: int):
    """The Silver text (cleaned text or Whisper transcript) as a .txt download."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT s.transcript, s.status_processing FROM silver.text s
                           JOIN bronze.documents b ON b.doc_id = s.doc_id WHERE s.doc_id = %s AND b.excluded = FALSE""", (doc_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None or row[1] != "success" or not row[0]:
        raise HTTPException(status_code=404, detail="No transcript yet: Silver processing has not succeeded for this document.")
    return PlainTextResponse(row[0], headers={"Content-Disposition": f'attachment; filename="document_{doc_id}.txt"'})
