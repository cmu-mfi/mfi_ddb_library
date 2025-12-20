"""
KV store connector.

This module provides a minimal, focused API for writing KV payloads into
the PostgreSQL `kv_store` table and reading them back.

Table definition (recommended):

    CREATE TABLE kv_store (
        id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        trial_id   TEXT NOT NULL,
        payload    JSONB NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );

Connection parameters are read from the following environment variables:

    KVSTORE_PGHOST, KVSTORE_PGPORT, KVSTORE_PGUSER,
    KVSTORE_PGPASSWORD, KVSTORE_PGDATABASE
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras


def _get_conn():
    params = {
        "host": os.environ.get("KVSTORE_PGHOST"),
        "port": int(os.environ.get("KVSTORE_PGPORT", "5432")),
        "user": os.environ.get("KVSTORE_PGUSER"),
        "password": os.environ.get("KVSTORE_PGPASSWORD"),
        "dbname": os.environ.get("KVSTORE_PGDATABASE"),
    }
    return psycopg2.connect(**params)


def insert_kv_item(trial_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Insert a new KV payload.

    The `payload` **must** contain the same `trial_id` as the first
    argument; this mirrors the schema in `schema/kv.json`.
    """
    if payload.get("trial_id") != trial_id:
        raise ValueError("payload['trial_id'] must match trial_id argument")

    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO kv_store (trial_id, payload)
                VALUES (%s, %s)
                RETURNING *;
                """,
                [trial_id, psycopg2.extras.Json(payload)],
            )
            row = cur.fetchone()
            conn.commit()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_kv_items(
    trial_id: Optional[str] = None, limit: int = 100
) -> List[Dict[str, Any]]:
    """
    Retrieve KV items.

    - If `trial_id` is provided, returns all items for that trial ordered
      by `created_at` descending.
    - Otherwise returns the most recent `limit` rows.
    """
    conn = _get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if trial_id:
                cur.execute(
                    """
                    SELECT * FROM kv_store
                    WHERE trial_id = %s
                    ORDER BY created_at DESC;
                    """,
                    [trial_id],
                )
            else:
                cur.execute(
                    """
                    SELECT * FROM kv_store
                    ORDER BY created_at DESC
                    LIMIT %s;
                    """,
                    [limit],
                )
            rows: Iterable[Dict[str, Any]] = cur.fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def latest_kv_item(trial_id: str) -> Optional[Dict[str, Any]]:
    """
    Convenience helper to fetch the most recent KV item for a trial.
    """
    items = get_kv_items(trial_id=trial_id, limit=1)
    return items[0] if items else None


