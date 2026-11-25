"""Postgres adapter for every database port the flow needs (runs, discovery log, Bronze registration, known URLs). Short-lived connections."""
from typing import Callable, Optional

from psycopg2.extras import Json

from prefect_flows.collection.enums import Mode, RunStatus
from prefect_flows.collection.recorder import DiscoveryEntry


class PostgresRepo:
    def __init__(self, conn_factory: Optional[Callable] = None):
        if conn_factory is None:
            from prefect_flows.clients import get_postgres_connection as conn_factory
        self._connect = conn_factory

    def _exec(self, sql: str, params=(), fetch: bool = False):
        conn = self._connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                row = cur.fetchone() if fetch else None
            conn.commit()
            return row
        finally:
            conn.close()

    # ---- runs -------------------------------------------------------------------------------------
    def ensure_run(self, run_id: str, mode: Mode, source_name: str, params: dict, params_version: int, dry_run: bool,
                   prefect_flow_run_id: Optional[str] = None, effective_config: Optional[dict] = None) -> None:
        """The backend normally creates the row (status queued); CLI runs create it here. Idempotent."""
        self._exec(
            """
            INSERT INTO collection.runs
                (run_id, prefect_flow_run_id, mode, source_name, params, params_version, dry_run, effective_config, status)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (run_id) DO UPDATE SET prefect_flow_run_id = COALESCE(EXCLUDED.prefect_flow_run_id, collection.runs.prefect_flow_run_id),
                                               effective_config = COALESCE(EXCLUDED.effective_config, collection.runs.effective_config)
            """,
            (run_id, prefect_flow_run_id, mode.value, source_name, Json(params), params_version, dry_run,
             Json(effective_config) if effective_config else None, RunStatus.QUEUED.value),
        )

    def mark_running(self, run_id: str) -> None:
        self._exec("UPDATE collection.runs SET status = %s, started_at = now() WHERE run_id = %s",
                   (RunStatus.RUNNING.value, run_id))

    def finalize_run(self, run_id: str, status: RunStatus, error_message: Optional[str] = None,
                     listing_pages_visited: int = 0) -> None:
        self._exec(
            """UPDATE collection.runs
               SET status = %s, finished_at = now(), error_message = %s,
                   listing_pages_visited = GREATEST(listing_pages_visited, %s)
               WHERE run_id = %s""",
            (status.value, (error_message or None) and error_message[:2000], listing_pages_visited, run_id),
        )

    def get_run_status(self, run_id: str) -> Optional[str]:
        row = self._exec("SELECT status FROM collection.runs WHERE run_id = %s", (run_id,), fetch=True)
        return row[0] if row else None

    # ---- discovery log (DiscoverySink) -------------------------------------------------------------
    def write_discovery(self, run_id: str, e: DiscoveryEntry) -> None:
        self._exec(
            """
            INSERT INTO bronze.discovery_log
                (run_id, source_name, source_url, canonical_url, listing_url, title_hint, status, reason_code,
                 detail, matched_keywords, match_scope, keywords_hash, doc_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (run_id, e.source_name, e.source_url, e.canonical_url, e.listing_url, e.title_hint, e.status.value,
             e.reason.value if e.reason else None, e.detail,
             Json(e.matched_keywords) if e.matched_keywords is not None else None,
             e.match_scope.value if e.match_scope else None, e.keywords_hash, e.doc_id),
        )

    # ---- Bronze ------------------------------------------------------------------------------------
    def checksum_exists(self, checksum: str) -> bool:
        return self._exec("SELECT 1 FROM bronze.documents WHERE checksum = %s", (checksum,), fetch=True) is not None

    def insert_document(self, *, source_url, source_type, file_name, file_size, mime_type, checksum,
                        raw_metadata, storage_path, run_id) -> Optional[int]:
        """Returns the new doc_id, or None if another writer registered the same checksum first."""
        row = self._exec(
            """
            INSERT INTO bronze.documents
                (source_url, source_type, file_name, file_size, mime_type, checksum, raw_metadata, storage_path, first_run_id)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (checksum) DO NOTHING
            RETURNING doc_id
            """,
            (source_url, source_type, file_name, file_size, mime_type, checksum,
             Json(raw_metadata) if raw_metadata else None, storage_path, run_id),
            fetch=True,
        )
        return row[0] if row else None

    # ---- KnownUrls port (used by research sources in phase 4) ----------------------------------------
    def known_url(self, canonical_url: str, source_type: Optional[str] = None) -> bool:
        """Known if Bronze already holds this URL (raw or canonical form), in this format when a format is given."""
        sql = ("SELECT 1 FROM bronze.documents WHERE (source_url = ANY(%s)"
               "OR raw_metadata->'collection'->>'canonical_url' = %s)")
        params = [[canonical_url, canonical_url + "/"], canonical_url]
        if source_type:
            sql += " AND source_type = %s"
            params.append(source_type)
        return self._exec(sql + " LIMIT 1", tuple(params), fetch=True) is not None


class MinioStore:
    """ObjectStore port over MinIO."""

    def __init__(self, client=None, bucket: str = "bronze"):
        if client is None:
            from prefect_flows.clients import get_minio_client
            client = get_minio_client()
        self.client, self.bucket = client, bucket

    def put(self, path, key: str, content_type: str) -> None:
        self.client.fput_object(bucket_name=self.bucket, object_name=key, file_path=str(path), content_type=content_type)
