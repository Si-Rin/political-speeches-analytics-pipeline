"""SQL for collection runs and the discovery log (API side). Plain psycopg2, short-lived connections."""
from typing import Any, Dict, List, Optional, Tuple

from psycopg2.extras import Json, RealDictCursor

from backend import db
from backend.schemas.common import RunStatus

_RUN_COLUMNS = ("run_id::text AS run_id, prefect_flow_run_id::text AS prefect_flow_run_id, mode, source_name, params, "
                "params_version, dry_run, status, listing_pages_visited, error_message, requested_at, started_at, finished_at")


def _fetch(sql: str, params: tuple = (), one: bool = False):
    conn = db.get_connection()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params)
            return cur.fetchone() if one else cur.fetchall()
    finally:
        conn.close()


def _execute(sql: str, params: tuple = ()) -> int:
    conn = db.get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            count = cur.rowcount
        conn.commit()
        return count
    finally:
        conn.close()


def create_run(run_id: str, mode: str, source_name: str, params: Dict[str, Any], params_version: int, dry_run: bool) -> None:
    _execute(
        """INSERT INTO collection.runs (run_id, mode, source_name, params, params_version, dry_run, status)
           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
        (run_id, mode, source_name, Json(params), params_version, dry_run, RunStatus.QUEUED.value),
    )


def set_prefect_id(run_id: str, prefect_flow_run_id: str) -> None:
    _execute("UPDATE collection.runs SET prefect_flow_run_id = %s WHERE run_id = %s", (prefect_flow_run_id, run_id))


def mark_terminal(run_id: str, status: RunStatus, error_message: Optional[str]) -> bool:
    """Move a non-terminal run to a terminal status. Never overwrites a result the flow already recorded."""
    return _execute(
        """UPDATE collection.runs SET status = %s, error_message = %s, finished_at = now()
           WHERE run_id = %s AND status IN ('queued', 'running')""",
        (status.value, (error_message or "")[:2000] or None, run_id),
    ) > 0


def get_run(run_id: str) -> Optional[Dict[str, Any]]:
    return _fetch(f"SELECT {_RUN_COLUMNS} FROM collection.runs WHERE run_id = %s", (run_id,), one=True)


def list_runs(limit: int = 50, status: Optional[str] = None) -> List[Dict[str, Any]]:
    sql = f"""SELECT r.run_id::text AS run_id, r.mode, r.source_name, r.dry_run, r.status, r.requested_at, r.finished_at,
                     (SELECT count(*) FROM bronze.discovery_log d WHERE d.run_id = r.run_id AND d.status = 'ingested') AS documents_ingested
              FROM collection.runs r"""
    params: List[Any] = []
    if status:
        sql += " WHERE r.status = %s"
        params.append(status)
    sql += " ORDER BY r.requested_at DESC LIMIT %s"
    params.append(limit)
    return _fetch(sql, tuple(params))


def summary(run_id: str) -> Tuple[Dict[str, int], Dict[str, int]]:
    """(counts by candidate status, counts by reason code) from the discovery log."""
    counts = {r["status"]: r["n"] for r in _fetch(
        "SELECT status, count(*) AS n FROM bronze.discovery_log WHERE run_id = %s GROUP BY status", (run_id,))}
    reasons = {r["reason_code"]: r["n"] for r in _fetch(
        """SELECT reason_code, count(*) AS n FROM bronze.discovery_log
           WHERE run_id = %s AND reason_code IS NOT NULL GROUP BY reason_code""", (run_id,))}
    return counts, reasons


def log_rows(run_id: str, status: Optional[str], reason_code: Optional[str], limit: int, offset: int) -> Tuple[int, List[Dict[str, Any]]]:
    where, params = "run_id = %s", [run_id]
    if status:
        where += " AND status = %s"
        params.append(status)
    if reason_code:
        where += " AND reason_code = %s"
        params.append(reason_code)
    total = _fetch(f"SELECT count(*) AS n FROM bronze.discovery_log WHERE {where}", tuple(params), one=True)["n"]
    rows = _fetch(
        f"""SELECT id, source_url, canonical_url, listing_url, title_hint, status, reason_code, detail,
                   matched_keywords, match_scope, doc_id, decided_at
            FROM bronze.discovery_log WHERE {where} ORDER BY id LIMIT %s OFFSET %s""",
        tuple(params + [limit, offset]))
    return total, rows
