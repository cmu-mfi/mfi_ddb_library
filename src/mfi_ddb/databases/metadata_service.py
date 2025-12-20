"""
Metadata database service.

This service is responsible for exposing REST endpoints directly backed
by the PostgreSQL metadata store. The Retrieval API talks to this
service over HTTP only.

Run (example):

    uvicorn mfi_ddb.databases.metadata_service:app --reload --port 8001

Environment:
    METADASTORE_PGHOST, METADASTORE_PGPORT,
    METADASTORE_PGUSER, METADASTORE_PGPASSWORD,
    METADASTORE_PGDATABASE
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query

from mfi_ddb.retrieval_api.metadata_store import metadata_utils


app = FastAPI(title="Metadata Service", version="0.1.0")


@app.get("/trials", summary="List trials from metadata store")
def list_trials(
    project_id: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    user_domain: Optional[str] = Query(default=None),
    trial_name: Optional[str] = Query(default=None),
) -> List[Dict[str, Any]]:
    """
    Read-only filter over `trial_detail` table.
    """
    conn = metadata_utils.get_conn_from_env()
    try:
        import psycopg2.extras

        where_clauses: List[str] = []
        params: List[Any] = []

        if project_id:
            where_clauses.append("project_id = %s")
            params.append(project_id)
        if user_id:
            where_clauses.append("user_id = %s")
            params.append(user_id)
        if user_domain:
            where_clauses.append("user_domain = %s")
            params.append(user_domain)
        if trial_name:
            where_clauses.append("trial_name = %s")
            params.append(trial_name)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"SELECT * FROM trial_detail {where_sql} ORDER BY created_at DESC",
                params,
            )
            rows = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


@app.get("/health", summary="Health check")
def health() -> Dict[str, str]:
    try:
        conn = metadata_utils.get_conn_from_env()
        conn.close()
    except Exception as exc:  # pragma: no cover - simple health check
        raise HTTPException(status_code=503, detail=str(exc))
    return {"status": "ok"}


