"""
Recover missing YouTube metadata for selected Bronze documents.

- Reads the YouTube URLs from bronze.documents
- Calls yt-dlp with download=False
- Does NOT download the audio/video again
- Updates bronze.raw_metadata
- Keeps the existing transcript and Bronze file untouched

Usage:
    python recover_youtube_metadata.py
"""

import json
import os
import sys

import psycopg2
import yt_dlp
from dotenv import load_dotenv
from psycopg2.extras import Json

load_dotenv()

# Documents to repair
DOC_IDS = [1, 2, 4]

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": os.getenv("POSTGRES_PORT", "5432"),
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}


def extract_youtube_metadata(url: str) -> dict:
    """
    Extract metadata from YouTube without downloading media.
    """
    ydl_opts = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
    }

    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    return {
        "title": info.get("title"),
        "description": info.get("description"),
        "upload_date": info.get("upload_date"),
        "channel": info.get("channel"),
        "channel_id": info.get("channel_id"),
        "uploader_url": info.get("uploader_url"),
        "duration": info.get("duration"),
        "view_count": info.get("view_count"),
        "like_count": info.get("like_count"),
        "tags": info.get("tags"),
        "categories": info.get("categories"),
        "webpage_url": info.get("webpage_url"),
        "language": info.get("language"),
    }


def main():
    conn = psycopg2.connect(**DB_CONFIG)

    try:
        with conn.cursor() as cur:

            # Get the existing YouTube URLs
            cur.execute(
                """
                SELECT doc_id, source_url, source_type
                FROM bronze.documents
                WHERE doc_id = ANY(%s)
                ORDER BY doc_id;
                """,
                (DOC_IDS,),
            )

            rows = cur.fetchall()

            if not rows:
                print("No documents found.")
                return

            for doc_id, source_url, source_type in rows:
                print("\n" + "=" * 70)
                print(f"doc_id      : {doc_id}")
                print(f"source_url  : {source_url}")
                print(f"source_type : {source_type}")
                print("=" * 70)

                if not source_url:
                    print("SKIP: source_url is empty.")
                    continue

                if "youtube.com" not in source_url and "youtu.be" not in source_url:
                    print("SKIP: URL is not recognized as a YouTube URL.")
                    continue

                try:
                    metadata = extract_youtube_metadata(source_url)

                    print("\nRecovered metadata:")
                    print(json.dumps(metadata, indent=2, ensure_ascii=False))

                    # Merge recovered metadata with any existing raw_metadata
                    cur.execute(
                        """
                        SELECT COALESCE(raw_metadata, '{}'::jsonb)
                        FROM bronze.documents
                        WHERE doc_id = %s;
                        """,
                        (doc_id,),
                    )

                    existing_metadata = cur.fetchone()[0] or {}
                    existing_metadata.update(metadata)

                    # Since the project is exclusively Donald Trump
                    existing_metadata["speaker"] = "Donald Trump"

                    # Update Bronze metadata only
                    cur.execute(
                        """
                        UPDATE bronze.documents
                        SET raw_metadata = %s
                        WHERE doc_id = %s;
                        """,
                        (Json(existing_metadata), doc_id),
                    )

                    print("\nOK: bronze.raw_metadata updated.")

                except Exception as exc:
                    print(f"\nERROR for doc_id={doc_id}: {exc}")

            conn.commit()
            print("\nAll updates committed.")

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()