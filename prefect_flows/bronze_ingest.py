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
    python prefect_flows/bronze_ingest.py --source local --folder /path/to/folder
    python prefect_flows/bronze_ingest.py --source urls --urls https://example.com/video1.mp4 https://example.com/video2.mp4
    python prefect_flows/bronze_ingest.py --source youtube --urls https://www.youtube.com/watch?v=abc123 https://www.youtube.com/watch?v=def456
    python prefect_flows/bronze_ingest.py --source youtube --urls https://www.youtube.com/watch?v=abc123 --audio-only --playlist-mode --max-downloads 10
    python prefect_flows/bronze_ingest.py --source miller_center --start-url https://millercenter.org/president/kennedy/speeches
    python prefect_flows/bronze_ingest.py --source miller_center --urls https://millercenter.org/president/kennedy/speeches https://millercenter.org/president/johnson/speeches
    python prefect_flows/bronze_ingest.py --source web_crawl --seed-urls https://example.com --keywords "politics" "speech" --allowed-domains example.com
"""
import argparse
import hashlib
import json
import os
import sys
from dotenv import load_dotenv
import tempfile
from pathlib import Path
from typing import Optional

import requests
from psycopg2.extras import Json
from minio.error import S3Error
from prefect import flow, task, get_run_logger

from prefect_flows.clients import get_minio_client, get_postgres_connection
from prefect_flows.sources.internet_archive import InternetArchiveSource
from prefect_flows.sources.single_item import SingleItemSource

sys.path.append(str(Path(__file__).resolve().parent.parent))  # so "sources.*" imports resolve
from prefect_flows.sources.base import Candidate
from prefect_flows.sources.local_folder import LocalFolderSource
from prefect_flows.sources.youtube import YoutubeSource
from prefect_flows.sources.miller_center import MillerCenterSource
from prefect_flows.sources.web_scraping import WebCrawlSource
from prefect_flows.sources.ucsb_tweets import UcsbTweetsSource

# Loads variables from a .env file in the current working directory (or any
# parent directory) into os.environ, if present. No-op inside Docker
# containers where the vars are already injected by docker-compose — but
# required when running this script directly from a local venv.
load_dotenv()

CHUNK_SIZE = 8 * 1024 * 1024  # 8MB streaming chunks


@task
def discover(source_name: str, **source_kwargs) -> list[Candidate]:
    logger = get_run_logger()
    if source_name == "local":
        source = LocalFolderSource(folder=source_kwargs["folder"])
    elif source_name == "youtube":
        source = YoutubeSource(
            urls=source_kwargs["urls"],
            audio_only=source_kwargs.get("audio_only", False),
            playlist_mode=source_kwargs.get("playlist_mode", False),
            max_downloads=source_kwargs.get("max_downloads"),
        )
    elif source_name == "web_crawl":
        source = WebCrawlSource(
            seed_urls=source_kwargs["seed_urls"],
            keywords=source_kwargs["keywords"],
            allowed_domains=source_kwargs.get("allowed_domains"),
            max_depth=source_kwargs.get("max_depth", 2),
            max_pages=source_kwargs.get("max_pages", 50),
        )
    elif source_name == "miller_center":
        source = MillerCenterSource(
            urls=source_kwargs.get("urls"),
            start_url=source_kwargs.get("start_url"),
            max_depth=source_kwargs.get("max_depth", 100),     
            crawl_delay=source_kwargs.get("crawl_delay", 5.0),
            request_timeout=source_kwargs.get("request_timeout", 10.0),
        )
    elif source_name == "ucsb_tweets":
        source = UcsbTweetsSource(
            listing_url=source_kwargs.get("listing_url"),
            link_text_filter=source_kwargs.get("link_text_filter", "tweets of"),
            max_documents=source_kwargs.get("max_documents", 1),
            crawl_delay=source_kwargs.get("crawl_delay", 1.0),
        )
    elif source_name=="internet_archive":
        source = InternetArchiveSource(
            urls=source_kwargs["urls"],
            excluded_indices=source_kwargs.get("excluded_indices", {})
        )
    elif source_name == "single":
        source = SingleItemSource(
            location=source_kwargs["location"],
            source_type=source_kwargs["content_type"],
            is_local=source_kwargs["is_local"],
            raw_metadata=source_kwargs.get("raw_metadata"),
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
    conn = get_postgres_connection()
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

        conn = get_postgres_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO bronze.documents
                        (source_url, source_type, file_name, file_size,
                         mime_type, checksum, raw_metadata, storage_path)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    RETURNING doc_id
                    """,
                    (
                        candidate.source_url,
                        candidate.source_type,
                        candidate.file_name,
                        file_size,
                        candidate.mime_type,
                        checksum,
                        Json(candidate.raw_metadata) if candidate.raw_metadata else None,
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
    parser.add_argument("--source", required=True, choices=["local", "miller_center", "youtube", "web_crawl", "ucsb_tweets", "internet_archive"], help="Source adapter to use for discovery")
    parser.add_argument("--folder", help="Folder path (required for --source local)")
    parser.add_argument("--url-file", help="Text file, one URL per line (alternative to --urls)")
    parser.add_argument("--urls", nargs="+", help="One or more URLs directly on the command line")
    parser.add_argument("--start-url", help="Starting URL for crawl mode (required for --source miller_center)")
    parser.add_argument("--type", dest="source_type", default="video", choices=["video", "audio", "text"], help="source_type to assign for --source urls")
    parser.add_argument("--audio-only", action="store_true", help="For --source youtube: download audio track only")
    parser.add_argument("--playlist-mode", action="store_true", help="For --source youtube: treat URLs as playlists and download all entries")
    parser.add_argument("--max-downloads", type=int, help="For --source youtube: maximum number of entries to download from each playlist")
    parser.add_argument("--seed-urls", nargs="+", help="Seed URLs for web crawl (required for --source web_crawl)")
    parser.add_argument("--keywords", nargs="+", help="Keywords for web crawl (required for --source web_crawl)")
    parser.add_argument("--allowed-domains", nargs="+", help="Allowed domains for web crawl (optional for --source web_crawl)")
    parser.add_argument("--max-depth", type=int, default=100, help="Maximum depth for web crawl and miller center (optional for --source web_crawl and --source miller_center)")
    parser.add_argument("--max-pages", type=int, default=50, help="Maximum pages for web crawl (optional for --source web_crawl)")
    parser.add_argument("--listing-url", help="President's document listing page (required for --source ucsb_tweets)") 
    parser.add_argument("--max-documents", type=int, default=1, help="Max tweet-day documents to yield for --source ucsb_tweets")
    parser.add_argument("--link-text-filter", default="tweets of", help="Anchor-text substring filter for --source ucsb_tweets")
    parser.add_argument("--excluded-indices", help="JSON file: {identifier: [idx, ...]} for --source internet_archive")
    parser.add_argument("--is-local", action="store_true", help="For --source single: --location is a local file path, not a URL")
    parser.add_argument("--raw-metadata", help="JSON string of probe-detected metadata to attach, for --source single")
    
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
    elif args.source == "youtube":
        ingest_bronze(source_name="youtube", 
            urls=_resolve_urls(), 
            audio_only=args.audio_only, 
            playlist_mode=args.playlist_mode, 
            max_downloads=args.max_downloads
        )
    elif args.source == "web_crawl":
        if not args.seed_urls or not args.keywords:
            parser.error("--seed-urls and --keywords are required for --source web_crawl")
        ingest_bronze(
            source_name="web_crawl",
            seed_urls=args.seed_urls,
            keywords=args.keywords,
            allowed_domains=args.allowed_domains,
            max_depth=args.max_depth,
            max_pages=args.max_pages
        )
    elif args.source == "miller_center":
        if not args.start_url and not args.urls:
            parser.error("--start-url or --urls is required for --source miller_center")
        ingest_bronze(
            source_name="miller_center",
            start_url=args.start_url,
            urls=args.urls,
            max_depth=args.max_depth
        )
    elif args.source == "ucsb_tweets":
        if not args.listing_url:
            parser.error("--listing-url is required for --source ucsb_tweets")
        ingest_bronze(
            source_name="ucsb_tweets",
            listing_url=args.listing_url,
            link_text_filter=args.link_text_filter,
            max_documents=args.max_documents,
        )
    elif args.source == "internet_archive":
        excluded = {}
        if args.excluded_indices:
            with open(args.excluded_indices, "r") as f:
                excluded = json.load(f)
        ingest_bronze(
            source_name="internet_archive",
            urls=_resolve_urls(),
            excluded_indices=excluded
        )
    elif args.source == "single":
        if not args.location:
            parser.error("--location is required for --source single")
        ingest_bronze(
            source_name="single",
            location=args.location,
            content_type=args.source_type,
            is_local=args.is_local,
            raw_metadata=json.loads(args.raw_metadata) if args.raw_metadata else None,
        )
            