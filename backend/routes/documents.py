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
from backend.routes.status import GOLD_MODULES
from backend.schemas import (
    ProbeRequest, ProbeResponse, DocumentSummary, DocumentHistoryResponse,
    DocumentRecord, DocumentLibraryResponse, DocumentDetail, TextPreview, MetadataUpdate, DeleteRequest, DeleteResponse,
)
from backend.pipeline_probe import probe



_GOLD_DONE_SQL = " + ".join(f"(g.{col} IS NOT NULL)::int" for col in GOLD_MODULES.values())
_RECORD_COLUMNS = f"""
    b.doc_id, b.source_url, b.file_name, b.source_type, b.file_size, b.mime_type, b.ingestion_date, b.raw_metadata->>'source_name',
    s.title, s.speaker, s.publication_date, s.language, s.word_count, s.status_processing, s.error_message,
    COALESCE({_GOLD_DONE_SQL}, 0)
"""
_RECORD_FROM = """
    FROM bronze.documents b
    LEFT JOIN silver.text s ON s.doc_id = b.doc_id
    LEFT JOIN gold.analytics g ON g.doc_id = b.doc_id
"""

def _record_kwargs(r) -> dict:
    (doc_id, source_url, file_name, source_type, file_size, mime_type, ingestion_date, origin,
     title, speaker, publication_date, language, word_count, status, error, gold_done) = r[:16]
    return dict(doc_id=doc_id, source=source_url or file_name or "", source_type=source_type, file_name=file_name,
                file_size=file_size, mime_type=mime_type, ingestion_date=ingestion_date, origin=origin, title=title,
                speaker=speaker, publication_date=publication_date, language=language, word_count=word_count,
                silver_status=status or "not_started", silver_error=error, gold_done=int(gold_done or 0), gold_total=len(GOLD_MODULES))


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



@router.get("/documents/library", response_model=DocumentLibraryResponse)
def get_library():
    """Every document with its metadata and processing state (Documents page: library and corpus statistics)."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT {_RECORD_COLUMNS} {_RECORD_FROM} WHERE b.excluded = FALSE ORDER BY b.ingestion_date DESC")
            rows = cur.fetchall()
    finally:
        conn.close()
    return DocumentLibraryResponse(documents=[DocumentRecord(**_record_kwargs(r)) for r in rows])


@router.post("/documents/delete", response_model=DeleteResponse)
def delete_documents(req: DeleteRequest):
    """Delete documents everywhere: Bronze row (Silver and Gold cascade) and the raw file in object storage.
    The URL becomes unknown again, so it can be collected anew."""
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze.documents WHERE doc_id = ANY(%s) RETURNING doc_id, storage_path", (list(set(req.doc_ids)),))
            deleted = cur.fetchall()
        conn.commit()
    finally:
        conn.close()
    storage_errors = 0
    for _doc_id, storage_path in deleted:
        try:
            if not storage.delete_bronze(storage_path):
                storage_errors += 1
        except storage.StorageUnavailable:
            storage_errors += 1
    return DeleteResponse(deleted=len(deleted), storage_errors=storage_errors)


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


@router.get("/documents/{doc_id}/detail", response_model=DocumentDetail)
def get_document_detail(doc_id: int):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT {_RECORD_COLUMNS}, b.checksum, b.storage_path, b.first_run_id::text, length(s.transcript),
                           s.processing_date, b.raw_metadata
                    {_RECORD_FROM} WHERE b.doc_id = %s AND b.excluded = FALSE""", (doc_id,))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Document {doc_id} not found")
    checksum, storage_path, first_run_id, char_count, processing_date, raw_metadata = row[16:22]
    return DocumentDetail(**_record_kwargs(row), checksum=checksum, storage_path=storage_path, first_run_id=first_run_id,
                          char_count=char_count, processing_date=processing_date, raw_metadata=raw_metadata)


@router.get("/documents/{doc_id}/text", response_model=TextPreview)
def get_text_preview(doc_id: int, offset: int = 0, limit: int = 5000):
    """A slice of the Silver text (transcript) for on-screen preview; the full text is /transcript."""
    offset, limit = max(offset, 0), min(max(limit, 1), 50000)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT substr(s.transcript, %s, %s), length(s.transcript)
                           FROM silver.text s JOIN bronze.documents b ON b.doc_id = s.doc_id
                           WHERE s.doc_id = %s AND b.excluded = FALSE AND s.transcript IS NOT NULL""", (offset + 1, limit, doc_id))
            row = cur.fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail="No text yet: Silver processing has not produced a text for this document.")
    text, total = row
    return TextPreview(text=text or "", offset=offset, total_chars=total or 0, has_more=offset + limit < (total or 0))


@router.patch("/documents/{doc_id}/metadata")
def update_metadata(doc_id: int, body: MetadataUpdate):
    """Edit title / speaker / publication date / language (Silver columns). Only the fields sent are changed."""
    fields = {k: getattr(body, k) for k in body.model_fields_set}
    if not fields:
        raise HTTPException(status_code=422, detail="Nothing to update.")
    assignments = ", ".join(f"{col} = %s" for col in fields)
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(f"UPDATE silver.text s SET {assignments} FROM bronze.documents b "
                        "WHERE s.doc_id = %s AND b.doc_id = s.doc_id AND b.excluded = FALSE RETURNING s.doc_id",
                        (*fields.values(), doc_id))
            updated = cur.fetchone()
        conn.commit()
    finally:
        conn.close()
    if updated is None:
        raise HTTPException(status_code=409, detail="Metadata can be edited once Silver has processed the document.")
    return {"ok": True, "updated": sorted(fields)}
