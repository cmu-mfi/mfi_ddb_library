"""
SQLite-backed persistence for connection configs and desired state.

Lets the backend survive a restart without losing track of which
connections existed and what state they should come back in (streaming,
paused, or stopped). Previously this only lived in the in-memory
active_connections dict in router.py and was lost on every restart.

Config is stored as opaque JSON blobs (adapter_cfg/streamer_cfg columns),
not individual typed columns - so a new field on some adapter's config
schema needs no migration here. The one thing that isn't handled for free
is an old saved blob failing validation against a since-changed, no-longer-
backward-compatible schema; callers should treat a restore failure for one
connection as non-fatal and skip just that one.
"""

import json
import logging
import sqlite3
from pathlib import Path
from typing import Any, Dict, List

logger = logging.getLogger(__name__)

DEFAULT_DB_PATH = Path("data/connections.db")


def _connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    return conn


def init_db(db_path: Path = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS connections (
                conn_id TEXT PRIMARY KEY,
                adapter_name TEXT NOT NULL,
                adapter_cfg TEXT NOT NULL,
                streamer_cfg TEXT NOT NULL,
                is_polling INTEGER NOT NULL,
                polling_rate_hz INTEGER NOT NULL,
                desired_state TEXT NOT NULL
            )
            """
        )


def save_connection(
    conn_id: str,
    adapter_name: str,
    adapter_cfg: Dict[str, Any],
    streamer_cfg: Dict[str, Any],
    is_polling: bool,
    polling_rate_hz: int,
    desired_state: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO connections
                (conn_id, adapter_name, adapter_cfg, streamer_cfg, is_polling, polling_rate_hz, desired_state)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(conn_id) DO UPDATE SET
                adapter_name=excluded.adapter_name,
                adapter_cfg=excluded.adapter_cfg,
                streamer_cfg=excluded.streamer_cfg,
                is_polling=excluded.is_polling,
                polling_rate_hz=excluded.polling_rate_hz,
                desired_state=excluded.desired_state
            """,
            (
                conn_id,
                adapter_name,
                json.dumps(adapter_cfg),
                json.dumps(streamer_cfg),
                int(is_polling),
                polling_rate_hz,
                desired_state,
            ),
        )


def update_desired_state(conn_id: str, desired_state: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE connections SET desired_state = ? WHERE conn_id = ?",
            (desired_state, conn_id),
        )


def update_polling_rate(conn_id: str, polling_rate_hz: int, db_path: Path = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        conn.execute(
            "UPDATE connections SET polling_rate_hz = ? WHERE conn_id = ?",
            (polling_rate_hz, conn_id),
        )


def delete_connection(conn_id: str, db_path: Path = DEFAULT_DB_PATH) -> None:
    with _connect(db_path) as conn:
        conn.execute("DELETE FROM connections WHERE conn_id = ?", (conn_id,))


def load_all_connections(db_path: Path = DEFAULT_DB_PATH) -> List[Dict[str, Any]]:
    with _connect(db_path) as conn:
        rows = conn.execute("SELECT * FROM connections").fetchall()

    result = []
    for row in rows:
        try:
            result.append({
                "conn_id": row["conn_id"],
                "adapter_name": row["adapter_name"],
                "adapter_cfg": json.loads(row["adapter_cfg"]),
                "streamer_cfg": json.loads(row["streamer_cfg"]),
                "is_polling": bool(row["is_polling"]),
                "polling_rate_hz": row["polling_rate_hz"],
                "desired_state": row["desired_state"],
            })
        except (json.JSONDecodeError, KeyError) as e:
            logger.warning("Skipping malformed saved connection row conn_id=%s: %s", row["conn_id"], e)
    return result
