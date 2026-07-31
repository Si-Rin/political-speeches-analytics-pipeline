# Infrastructure validation
import os
import psycopg2
from minio import Minio
from prefect_flows import flow, task, get_run_logger
from dotenv import load_dotenv

load_dotenv()

@task(retries=3, retry_delay_seconds=5)
def check_minio() -> str:
    logger = get_run_logger()
    client = Minio(
        os.environ.get("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.environ["MINIO_ROOT_USER"],
        secret_key=os.environ["MINIO_ROOT_PASSWORD"],
        secure=False,
    )
    found  = client.bucket_exists("bronze")
    if not found:
        raise RuntimeError("Bucket 'bronze' does not exist — check minio-init service logs")
    logger.info("MinIO OK — 'bronze' bucket is present")
    return "minio_ok"

@task(retries=3, retry_delay_seconds=5)
def check_postgres() -> str:
    logger = get_run_logger()
    conn = psycopg2.connect(
        host=os.environ.get("POSTGRES_HOST", "postgres"),
        port=os.environ.get("POSTGRES_PORT", "5433"),
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        dbname=os.environ["POSTGRES_DB"],
        client_encoding='utf8',  # Force PostgreSQL to communicate in UTF-8
        options="-c lc_messages=C"   # Force PostgreSQL to use C locale for messages (avoids locale issues)
    )

    with conn.cursor() as cur: 
        cur.execute(
            """
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold')
            ORDER BY table_schema, table_name;
            """
        )
        rows = cur.fetchall()
    conn.close()
    
    expected = {
        ("bronze", "documents"),
        ("silver", "text"),
        ("gold", "analytics"),
    }
    found = {(r[0], r[1]) for r in rows}
    missing = expected - found
    if missing:
        raise RuntimeError(f"Missing expected tables: {missing}")
    logger.info(f"Postgres OK — found tables: {sorted(found)}")
    return "postgres_ok"

@flow(name="infra_health_check", description="Check that MinIO and Postgres are available and healthy")
def health_check():
    logger = get_run_logger()
    minio_status = check_minio()
    postgres_status = check_postgres()
    logger.info("Infrastructure health check completed successfully")
    
if __name__ == "__main__":
    health_check()