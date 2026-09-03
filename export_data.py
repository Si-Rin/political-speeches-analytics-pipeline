"""
Export collected doc URLs (bronze layer) and their transcripts (silver layer) to a CSV file

Connects to Postgres using credentials from a .env file (python-dotenv)
Joins bronze and silver on a shared doc ID column (doc_id)
Rows with no transcript yet are still included, with the transcript left blank

Usage:
    python export_data.py --output docs_transcripts.xlsx
"""

import argparse
import openpyxl
import sys

import psycopg2
import psycopg2.extras

from prefect_flows.clients import get_postgres_connection

BRONZE_TABLE = "bronze.documents"
SILVER_TABLE = "silver.text"

BRONZE_ID_COL = "doc_id"  
SILVER_ID_COL = "doc_id"  

BRONZE_URL_COL = "source_url" 
SILVER_TRANSCRIPT_COL = "transcript" 

EXCEL_COLUMNS = [ "doc_id", "source_url", "transcript", "transcript_available", ]

# ---------------------------------------------------------------------------


def fetch_rows(conn):
    query = f"""
        SELECT
            b.{BRONZE_ID_COL}      AS doc_id,
            b.{BRONZE_URL_COL}     AS source_url,
            s.{SILVER_TRANSCRIPT_COL} AS transcript
        FROM {BRONZE_TABLE} b
        LEFT JOIN {SILVER_TABLE} s
            ON b.{BRONZE_ID_COL} = s.{SILVER_ID_COL}
        ORDER BY b.{BRONZE_ID_COL};
    """

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()


def write_excel(rows, output_path):
    workbook = openpyxl.Workbook() 
    worksheet = workbook.active 
    worksheet.title = "Documents" 
    worksheet.append(EXCEL_COLUMNS)
    
    n_with_transcript = 0
    for row in rows:
        has_transcript = row["transcript"] is not None and row["transcript"] != ""
        if has_transcript:
            n_with_transcript += 1

        worksheet.append(
            [
                row["doc_id"],
                row["source_url"],
                row["transcript"] or "",
                "yes" if has_transcript else "no",
            ]
        )
            
    # Adjust column widths 
    worksheet.column_dimensions["A"].width = 12 
    worksheet.column_dimensions["B"].width = 80 
    worksheet.column_dimensions["C"].width = 200
    worksheet.column_dimensions["D"].width = 22 
    # Freeze the header row 
    worksheet.freeze_panes = "A2" 
    workbook.save(output_path)
    return len(rows), n_with_transcript


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output",
        default="docs_transcripts.xlsx",
        help="Path to the output CSV file (default: docs_transcripts.xlsx)",
    )
    args = parser.parse_args()

    try:
        conn = get_postgres_connection()
    except psycopg2.OperationalError as e:
        print(f"Failed to connect to Postgres: {e}", file=sys.stderr)
        sys.exit(1)

    try:
        rows = fetch_rows(conn)
    finally:
        conn.close()

    total, with_transcript = write_excel(rows, args.output)

    print(f"Wrote {total} rows to {args.output}")
    print(f"  {with_transcript} with transcript")
    print(f"  {total - with_transcript} missing transcript")


if __name__ == "__main__":
    main()