"""
Users for 'user' fields, read from the metadata store (ddb_user table).

A field of type `user` is a required pick from the people already registered
in the MFI DDB metadata store - never a free-text or default value. The list
is read (read-only) from PostgreSQL and cached for a few seconds, so a person
registered through the "New user" form shows up in the dropdown shortly after
the metadata-store connector has written them.

Connection settings come from the `users.database` section of the YAML
config (env-interpolated, so the password can live in .env).
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

UserKey = Tuple[str, str]  # (user_id, domain)

_QUERY = "SELECT user_id, domain, name FROM ddb_user ORDER BY lower(coalesce(name, user_id)), user_id, domain"


class UserDirectoryError(Exception):
    """The user list could not be loaded from the metadata store."""


class UserDirectory:
    """Cached, read-only view of ddb_user."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._users: Optional[List[dict]] = None
        self._loaded_at = 0.0
        self._loaded_for: Optional[tuple] = None
        self.error: Optional[str] = None

    @staticmethod
    def _fetch(db: dict, timeout: float) -> List[dict]:
        import psycopg2  # local import: tests and tooling don't need a driver

        conn = psycopg2.connect(
            host=db["host"],
            port=db["port"],
            dbname=db["dbname"],
            user=db["user"],
            password=db["password"],
            connect_timeout=max(1, int(timeout)),
            options="-c default_transaction_read_only=on",
        )
        try:
            with conn.cursor() as cur:
                cur.execute(_QUERY)
                rows = cur.fetchall()
        finally:
            conn.close()
        return [
            {"user_id": user_id, "domain": domain, "name": name or ""}
            for user_id, domain, name in rows
        ]

    def list(self, users_cfg, force: bool = False) -> List[dict]:
        """All users, newest list at most `cache_seconds` old. Raises UserDirectoryError."""
        db = users_cfg.database.model_dump()
        signature = tuple(sorted(db.items()))
        with self._lock:
            fresh = (
                self._users is not None
                and self._loaded_for == signature
                and time.monotonic() - self._loaded_at < users_cfg.cache_seconds
            )
            if fresh and not force:
                return self._users
            try:
                users = self._fetch(db, users_cfg.connect_timeout_seconds)
            except Exception as err:
                self.error = (
                    f"Cannot load users from the metadata store at "
                    f"{db['host']}:{db['port']}/{db['dbname']}: {str(err).strip()}"
                )
                logger.warning(self.error)
                raise UserDirectoryError(self.error) from None
            self._users, self._loaded_at, self._loaded_for, self.error = users, time.monotonic(), signature, None
            return users

    def lookup(self, users_cfg) -> Dict[UserKey, dict]:
        return {(u["user_id"], u["domain"]): u for u in self.list(users_cfg)}


directory = UserDirectory()
