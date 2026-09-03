import dotenv
import re
import psycopg2

from prefect_flows.clients import get_postgres_connection


# Matches paths such as:
# C:\Users\hp\AppData\Local\Temp\youtube_dl_9uvywh5v\ydE7Gkl6MmI.webm
# C:\Users\hp\AppData\Local\Temp\youtube_dl_xxxxxxxx\oUEaSaJnULY.mp4
YOUTUBE_PATH_PATTERN = re.compile(
    r"youtube_dl_[^\\/]+[\\/](?P<video_id>[^\\/]+)\.(?:webm|mp4|mkv|mp3|m4a)$",
    re.IGNORECASE
)

def extract_youtube_url(source_url: str):
    """
    Extract the YouTube video ID from a temporary youtube_dl path
    and convert it to a canonical YouTube watch URL.
    """

    if not source_url:
        return None

    match = YOUTUBE_PATH_PATTERN.search(source_url)

    if not match:
        return None

    video_id = match.group("video_id")

    return f"https://www.youtube.com/watch?v={video_id}"


def fix_youtube_urls(dry_run=True):
    conn = None

    try:
        conn = get_postgres_connection()
        cursor = conn.cursor()

        # Get only URLs that look like temporary youtube_dl paths
        cursor.execute("""
            SELECT doc_id, source_url
            FROM bronze.documents
            WHERE source_url LIKE '%youtube_dl_%';
        """)

        rows = cursor.fetchall()

        print(f"Found {len(rows)} possible YouTube paths.\n")

        updates = []

        for doc_id, old_url in rows:
            new_url = extract_youtube_url(old_url)

            if new_url:
                updates.append((doc_id, old_url, new_url))

        # Preview
        print("Proposed changes:")
        print("-" * 100)

        for doc_id, old_url, new_url in updates:
            print(f"doc_id: {doc_id}")
            print(f"OLD: {old_url}")
            print(f"NEW: {new_url}")
            print("-" * 100)

        print(f"\nValid YouTube URLs reconstructed: {len(updates)}")

        if dry_run:
            print("\nDRY RUN: No database changes were made.")
            return

        # Actually update the database
        for doc_id, old_url, new_url in updates:
            cursor.execute(
                """
                UPDATE bronze.documents
                SET source_url = %s
                WHERE doc_id = %s;
                """,
                (new_url, doc_id)
            )

        conn.commit()

        print(f"\nSuccessfully updated {len(updates)} rows.")

    except Exception as e:
        if conn:
            conn.rollback()

        print(f"Error: {e}")

    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    # First run: preview only
    fix_youtube_urls(dry_run=False)
