"""
Bronze ingestion flow.

For each candidate discovered by a source adapter:
  1. Stream its bytes to a local temp file while computing a SHA-256 checksum
     (works the same whether the candidate is a local file or a remote URL).
  2. Skip it if a document with that checksum is already in bronze.documents
     (dedup — same file re-ingested is a no-op).
  3. Upload the bytes to MinIO under bronze/<checksum><ext>.
  4. Insert a row into bronze.documents referencing that object key.

Usage:
    python flows/ingest_bronze.py --source local --folder /data/speeches
    python flows/ingest_bronze.py --source urls --url-file urls.txt --type video
    python flows/ingest_bronze.py --source youtube --url-file youtube_urls.txt --audio-only
"""
import argparse
import hashlib
import os
import sys
from dotenv import load_dotenv
import tempfile
from pathlib import Path
from typing import Optional

import psycopg2
import requests
from minio import Minio
from minio.error import S3Error
from prefect import flow, task, get_run_logger

sys.path.append(str(Path(__file__).resolve().parent.parent))  # so "sources.*" imports resolve
from prefect_flows.sources.base import Candidate
from prefect_flows.sources.local_folder import LocalFolderSource
from prefect_flows.sources.url_s import UrlListSource
from prefect_flows.sources.youtube import YoutubeSource

# Loads variables from a .env file in the current working directory (or any
# parent directory) into os.environ, if present. No-op inside Docker
# containers where the vars are already injected by docker-compose — but
# required when running this script directly from a local venv.
load_dotenv()

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB streaming chunks


def get_minio_client() -> Minio:
    return Minio(
        os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )


def get_pg_conn():  
    return psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "127.0.0.1"),
        port=os.environ.get("POSTGRES_PORT", "5433"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        client_encoding='utf8',  # Force PostgreSQL to communicate in UTF-8
        options="-c lc_messages=C",    # Force PostgreSQL to use C locale for messages (avoids locale issues)
        gssencmode="disable"  # skip Windows SSPI/GSSAPI negotiation
    )


@task
def discover(source_name: str, **source_kwargs) -> list[Candidate]:
    logger = get_run_logger()
    if source_name == "local":
        source = LocalFolderSource(folder=source_kwargs["folder"])
    elif source_name == "urls":
        source = UrlListSource(
            urls=source_kwargs["urls"],
            source_type=source_kwargs.get("source_type", "video"),
        )
    elif source_name == "youtube":
        source = YoutubeSource(
            urls=source_kwargs["urls"],
            audio_only=source_kwargs.get("audio_only", False),
        )
    else:
        raise ValueError(f"Unknown source '{source_name}'")

    candidates = list(source.discover())
    logger.info(f"Discovered {len(candidates)} candidate(s) from source '{source_name}'")
    return candidates


@task(retries=2, retry_delay_seconds=10)
def checksum_and_stage(candidate: Candidate) -> dict:
    """
    Streams the candidate's bytes to a local temp file while computing a
    SHA-256 checksum. Does not touch MinIO or Postgres yet — that only
    happens after we know it's not a duplicate.
    """
    logger = get_run_logger()
    sha256 = hashlib.sha256()
    tmp_fd, tmp_path = tempfile.mkstemp(prefix="bronze_")

    try:
        with os.fdopen(tmp_fd, "wb") as tmp_file:
            if candidate.is_local:
                with open(candidate.source_url, "rb") as src:
                    while chunk := src.read(CHUNK_SIZE):
                        sha256.update(chunk)
                        tmp_file.write(chunk)
            else:
                with requests.get(candidate.source_url, stream=True, timeout=30) as resp:
                    resp.raise_for_status()
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        sha256.update(chunk)
                        tmp_file.write(chunk)

        file_size = os.path.getsize(tmp_path)
        checksum = sha256.hexdigest()
        logger.info(f"Staged '{candidate.file_name}' ({file_size} bytes, checksum={checksum[:12]}...)")

        return {
            "candidate": candidate,
            "tmp_path": tmp_path,
            "checksum": checksum,
            "file_size": file_size,
        }
    except Exception:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)  # don't leak temp files on failure
        raise


@task
def is_duplicate(checksum: str) -> bool:
    conn = get_pg_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM bronze.documents WHERE checksum = %s", (checksum,))
            return cur.fetchone() is not None
    finally:
        conn.close()


@task
def upload_and_register(staged: dict) -> Optional[int]:
    """Uploads the staged file to MinIO, inserts the bronze.documents row,
    and always cleans up the local temp file (success or failure)."""
    logger = get_run_logger()
    candidate: Candidate = staged["candidate"]
    tmp_path = staged["tmp_path"]
    checksum = staged["checksum"]
    file_size = staged["file_size"]

    try:
        ext = Path(candidate.file_name).suffix
        object_key = f"{checksum}{ext}"

        client = get_minio_client()
        client.fput_object(
            bucket_name="bronze",
            object_name=object_key,
            file_path=tmp_path,
            content_type=candidate.mime_type or "application/octet-stream",
        )

        conn = get_pg_conn()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bronze.documents
                        (source_url, source_type, file_name, file_size,
                         mime_type, checksum, storage_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING doc_id
                    """,
                    (
                        candidate.source_url,
                        candidate.source_type,
                        candidate.file_name,
                        file_size,
                        candidate.mime_type,
                        checksum,
                        object_key,
                    ),
                )
                doc_id = cur.fetchone()[0]
            conn.commit()
        finally:
            conn.close()

        logger.info(f"Ingested '{candidate.file_name}' as doc_id={doc_id} (bronze/{object_key})")
        return doc_id

    except S3Error as e:
        logger.error(f"MinIO upload failed for '{candidate.file_name}': {e}")
        raise
    finally:
        if os.path.exists(tmp_path):
            os.remove(tmp_path)


@flow(name="bronze-ingestion")
def ingest_bronze(source_name: str, **source_kwargs):
    logger = get_run_logger()
    candidates = discover(source_name, **source_kwargs)

    ingested, skipped, failed = 0, 0, 0
    for candidate in candidates:
        try:
            staged = checksum_and_stage(candidate)
            if is_duplicate(staged["checksum"]):
                logger.info(f"Skipping duplicate: '{candidate.file_name}'")
                os.remove(staged["tmp_path"])
                skipped += 1
                continue
            doc_id = upload_and_register(staged)
            if doc_id is not None:
                ingested += 1
        except Exception as e:
            logger.error(f"Failed to ingest '{candidate.file_name}': {e}")
            failed += 1

    logger.info(f"Bronze ingestion complete: {ingested} ingested, {skipped} skipped (dup), {failed} failed")
    return {"ingested": ingested, "skipped": skipped, "failed": failed}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", required=True, choices=["local", "urls", "youtube"])
    parser.add_argument("--folder", help="Folder path (required for --source local)")
    parser.add_argument("--url-file", help="Text file, one URL per line (alternative to --urls)")
    parser.add_argument("--urls", nargs="+", help="One or more URLs directly on the command line")
    parser.add_argument("--type", dest="source_type", default="video", choices=["video", "audio", "text"],
                         help="source_type to assign for --source urls")
    parser.add_argument("--audio-only", action="store_true",
                         help="For --source youtube: download audio track only")
    args = parser.parse_args()

    def _resolve_urls():
        if args.urls:
            return args.urls
        if args.url_file:
            with open(args.url_file) as f:
                return [line.strip() for line in f if line.strip()]
        parser.error("Provide URLs via --urls or --url-file")

    if args.source == "local":
        if not args.folder:
            parser.error("--folder is required for --source local")
        ingest_bronze(source_name="local", folder=args.folder)
    elif args.source == "urls":
        ingest_bronze(source_name="urls", urls=_resolve_urls(), source_type=args.source_type)
    elif args.source == "youtube":
        ingest_bronze(source_name="youtube", urls=_resolve_urls(), audio_only=args.audio_only)
