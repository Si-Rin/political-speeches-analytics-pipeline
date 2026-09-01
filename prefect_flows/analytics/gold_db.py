"""
Shared Gold-layer persistence.

upsert_gold_analytics() is the single write path for every Gold flow (gold_linguistic.py, gold_sentiment.py, gold_labels.py)
Each flow computes its own JSONB payload(s) via its own analytics module(s) — the ML/analytics logic never lives here — and passes them as kwargs
This function only knows how to build a targeted INSERT ... ON CONFLICT DO UPDATE SET for gold.analytics
So multiple flows can each populate their own column(s) on the same row without clobbering one another.
"""
from psycopg2.extras import Json

from prefect import task

from prefect_flows.clients import get_postgres_connection


@task
def upsert_gold_analytics(doc_id: int, **fields):
    """
    Pass any subset of gold.analytics' JSONB columns as kwargs (e.g. lex_metrics=..., sentiment=..., emotions=...) 
    Only those columns are written; every other Gold flow's columns on that row are left untouched
    """
    if not fields:
        return

    columns = list(fields.keys())
    conn = get_postgres_connection()
    try:
        with conn.cursor() as cur:
            insert_cols = ", ".join(["doc_id", *columns, "analysis_date"])
            insert_placeholders = ", ".join(["%s"] * (len(columns) + 1) + ["now()"])
            update_set = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns] + ["analysis_date = now()"])
            values = [doc_id] + [Json(fields[col]) for col in columns]

            cur.execute(
                f"""
                INSERT INTO gold.analytics ({insert_cols})
                VALUES ({insert_placeholders})
                ON CONFLICT (doc_id) DO UPDATE SET {update_set}
                """,
                values,
            )
        conn.commit()
    finally:
        conn.close()