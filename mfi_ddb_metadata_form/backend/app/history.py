"""Small SQLite log of submissions (stored on the backend_data volume)."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

_lock = threading.Lock()


class History:
    def __init__(self, db_path: Path) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS submissions (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at  TEXT NOT NULL,
                    form_id     TEXT NOT NULL,
                    msg_type    TEXT NOT NULL,
                    status      TEXT NOT NULL,
                    topic       TEXT,
                    payload     TEXT,
                    error       TEXT
                )
                """
            )

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def add(
        self,
        form_id: str,
        msg_type: str,
        status: str,
        payload: Optional[dict],
        topic: Optional[str] = None,
        error: Optional[str] = None,
        max_entries: int = 500,
    ) -> int:
        created_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        with _lock, self._connect() as conn:
            cur = conn.execute(
                "INSERT INTO submissions (created_at, form_id, msg_type, status, topic, payload, error)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    created_at,
                    form_id,
                    msg_type,
                    status,
                    topic,
                    json.dumps(payload) if payload is not None else None,
                    error,
                ),
            )
            conn.execute(
                "DELETE FROM submissions WHERE id NOT IN "
                "(SELECT id FROM submissions ORDER BY id DESC LIMIT ?)",
                (max_entries,),
            )
            return int(cur.lastrowid)

    def recent(self, limit: int = 25) -> List[dict]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM submissions ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        result = []
        for row in rows:
            item = dict(row)
            item["payload"] = json.loads(item["payload"]) if item["payload"] else None
            result.append(item)
        return result
