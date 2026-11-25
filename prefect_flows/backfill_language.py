"""
One-off: fill silver.text.language for rows processed before the column existed.

Order of preference per document: language typed by the researcher (raw_metadata.user_provided.language),
else detected from the transcript. Rows whose text is too short to detect stay NULL (they can be edited on the Documents page).

    docker compose exec prefect-worker python -m prefect_flows.backfill_language --dry-run
    docker compose exec prefect-worker python -m prefect_flows.backfill_language
"""
import argparse
from collections import Counter
from typing import Optional

from prefect_flows.clients import get_postgres_connection
from prefect_flows.extractors.utils import detect_language, normalize_language_code


def pick_language(raw_metadata: Optional[dict], transcript: Optional[str]) -> Optional[str]:
    user = ((raw_metadata or {}).get("user_provided") or {})
    return normalize_language_code(user.get("language")) or detect_language(transcript)


def backfill(dry_run: bool = False) -> Counter:
    conn = get_postgres_connection()
    found = Counter()
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT s.doc_id, b.raw_metadata, s.transcript FROM silver.text s JOIN bronze.documents b ON b.doc_id = s.doc_id
                           WHERE s.language IS NULL AND s.transcript IS NOT NULL""")
            rows = cur.fetchall()
            updates = []
            for doc_id, raw_metadata, transcript in rows:
                language = pick_language(raw_metadata, transcript)
                found[language or "undetected"] += 1
                if language:
                    updates.append((language, doc_id))
            if not dry_run and updates:
                cur.executemany("UPDATE silver.text SET language = %s WHERE doc_id = %s AND language IS NULL", updates)
        if not dry_run:
            conn.commit()
    finally:
        conn.close()
    return found


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Only show what would be written")
    args = parser.parse_args()
    result = backfill(dry_run=args.dry_run)
    print(("DRY RUN - " if args.dry_run else "") + f"{sum(result.values())} document(s): " + ", ".join(f"{k}={v}" for k, v in result.most_common()))
