"""
Silver ingestion flow.

For each undone row in bronze.documents:
  1. Download the raw file from MinIO (audio/video, or HTML page)
  2. Extract clean text:
     - video/audio -> faster-whisper transcription
     - text (Miller Center) -> extract main content from HTML using selectors 
    - text (generic crawl) -> trafilatura main-content extraction
    3. Derive publication_date/title from raw_metadata if present, else using regex/trafilatura, calculate word_count
    4. Store the clean text and metadata in silver.documents
"""

import os
import argparse
import re
import sys
import tempfile
from pathlib import Path
from typing import Optional
from minio import S3Error
from prefect import flow, task, get_run_logger

from prefect_flows.clients import get_minio_client, get_postgres_connection
from prefect_flows.extractors.base import SilverRecord
from prefect_flows.extractors.factory import get_extractor

sys.path.append(str(Path(__file__).resolve().parent.parent))

@task
def get_pending_documents(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None) -> list[dict]:
  conn = get_postgres_connection()
  try:
    with conn.cursor() as cur:
      base = """
          SELECT b.doc_id, b.source_type, b.storage_path, b.raw_metadata
          FROM bronze.documents b
          LEFT JOIN silver.text s ON s.doc_id = b.doc_id
          WHERE (s.doc_id IS NULL OR s.status_processing != 'success') AND b.excluded = FALSE
      """
      if doc_ids:
        cur.execute(base+" AND b.doc_id = ANY(%s)", (doc_ids,))
      elif limit:
        cur.execute(base + " ORDER BY b.doc_id LIMIT %s", (limit,))
      else:
          cur.execute(base + " ORDER BY b.doc_id")
      rows = cur.fetchall()
      return [{"doc_id": r[0], "source_type": r[1], "storage_path": r[2], "raw_metadata": r[3] or {}} for r in rows]
  finally:
      conn.close()
      
@task(retries=1)
def download_from_minio(storage_path: str) -> str:
    """
    Downloads a Bronze object to a local temp file

    Returns None (no exception raised, no retry attempted) if the object no longer exists in MinIO (manually deleted after being found corrupted)
    Genuine transient errors (network blip, timeout) still raise normally and retry
    """
    logger = get_run_logger()
    client = get_minio_client()
    suffix = Path(storage_path).suffix  # file extension as 'string'
    # Create a temporary file "silver_filename.extension"
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="silver_", suffix=suffix)
    # Close the file descriptor to prevent its leak
    os.close(tmp_fd)
    # Download the object data from bronze bucket to the temporary file
    try:
        client.fget_object(bucket_name="bronze", object_name=storage_path, file_path=tmp_path)
        return tmp_path
    except S3Error as e:
        if e.code in ("NoSuchKey", "NoSuchObject", "NoSuchBucket"):
            logger.warning(f"Bronze object missing (likely deleted): '{storage_path}'")
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
            return None
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise  # any other S3 error is unexpected — still retry normally
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)
        raise
  
@task
def extract(local_path: str, source_type: str, raw_metadata: dict):
    extractor = get_extractor(source_type, raw_metadata)
    return extractor.extract(local_path, raw_metadata)  # extract content -> title -> date

@task
def upsert_silver(record: SilverRecord):
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO silver.text (doc_id, title, publication_date, transcript, word_count, status_processing, processing_date, error_message)
                VALUES (%s, %s, %s, %s, %s, %s, now(), %s)
                ON CONFLICT (doc_id) DO UPDATE SET
                    title = EXCLUDED.title, publication_date = EXCLUDED.publication_date,
                    transcript = EXCLUDED.transcript, word_count = EXCLUDED.word_count,
                    status_processing = EXCLUDED.status_processing,
                    processing_date = EXCLUDED.processing_date, error_message = EXCLUDED.error_message
                """,
                (record.doc_id, record.title, record.publication_date, record.transcript, record.word_count, record.status_processing, record.error_message),
            )
        conn.commit()
    finally:
        conn.close()

@flow(name="silver-ingestion")
def ingest_silver(limit: Optional[int] = None, doc_ids: Optional[list[int]] = None):
    logger = get_run_logger()
    documents = get_pending_documents(limit=limit, doc_ids=doc_ids)
    logger.info(f"Processing {len(documents)} document(s)")

    succeeded, failed = 0, 0
    for doc in documents:
        local_path = None
        try:
            local_path = download_from_minio(doc["storage_path"])
            if not local_path:
                upsert_silver(SilverRecord.failure(doc["doc_id"], "Bronze object missing from MinIO (deleted; likely corrupted)"))
                failed += 1
                logger.error(f"doc_id={doc['doc_id']}: Bronze object missing, marked failed")
                continue
            content = extract(local_path, doc["source_type"], doc["raw_metadata"])
            record = SilverRecord.success(doc["doc_id"], content)
            upsert_silver(record)
            succeeded += 1
            logger.info(f"doc_id={doc['doc_id']}: success ({record.word_count} words)")
        except Exception as e:
            upsert_silver(SilverRecord.failure(doc["doc_id"], str(e)))
            failed += 1
            logger.error(f"doc_id={doc['doc_id']}: failed - {e}")
        finally:
            if local_path and os.path.exists(local_path):
                os.remove(local_path)

    logger.info(f"Silver ingestion complete: {succeeded} succeeded, {failed} failed")
    return {"succeeded": succeeded, "failed": failed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int, help="Process only N pending documents (dry-run batch)")
    parser.add_argument("--doc-ids", type=int, nargs="+", help="Process specific doc_id(s) only")
    args = parser.parse_args()
    ingest_silver(limit=args.limit, doc_ids=args.doc_ids)