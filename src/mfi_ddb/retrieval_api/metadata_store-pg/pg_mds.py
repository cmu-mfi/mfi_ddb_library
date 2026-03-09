"""
Reusable utilities for inserting rows into the metadata store tables.

Design considerations:
- Safe SQL composition (psycopg2.sql) to avoid injection via table/column names
- Generic `_upsert` helper that omits columns when value is None so DB defaults apply
- Per-table convenience wrappers that map higher-level arguments into columns

Usage example:
    from mfi_ddb.scripts import metadata_utils as mu
    row = mu.insert_user('alice', 'example.com', email='a@x.com')
    print(row)

Requires: psycopg2
    pip install psycopg2-binary
"""

from typing import Optional, Tuple, Dict, Any, List
import os
import psycopg2
from psycopg2 import sql
import psycopg2.extras
from pg_config import load_config

_ALLOWED_TABLES = {
    "user",
    "project",
    "trial",
    "user_project_role_linking",
    "graph_edges",
}

logger = logging.getLogger(__name__)


class MdsConnector:
    def __init__(self):
        self.__conn_pool = None
        try:
            config = load_config()
            self.__conn_pool = psycopg2.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                **config
            )
            logger.info("Database connection pool created successfully")
        except Exception as error:
            logger.error(
                f"Error occurred while creating connection pool: {error}. \n Config used: {config}"
            )
            raise error

    def _validate_table(self, table: str):
        if table not in _ALLOWED_TABLES:
            raise ValueError(
                f"Table '{table}' is not allowed. Allowed: {sorted(_ALLOWED_TABLES)}"
            )

    def _upsert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        """Generic upsert helper.

        - Omits keys with value is None so DB defaults (e.g., DEFAULT gen_random_uuid()) apply.
        - Uses psycopg2.sql to safely compose identifiers.
        - Returns the upserted row as a dict.
        """
        self._validate_table(table)

        # Filter out None values so DB defaults apply
        cols = [k for k, v in values.items() if v is not None]
        vals = [values[k] for k in cols]

        if not cols:
            raise ValueError("No columns to insert")

        # Convert python dict/list to JSON wrapper for psycopg2
        processed_vals: List[Any] = []
        for v in vals:
            if isinstance(v, (dict, list)):
                processed_vals.append(psycopg2.extras.Json(v))
            else:
                processed_vals.append(v)

        placeholders = [sql.Placeholder() for _ in cols]

        query = sql.SQL(
            "INSERT INTO {table} \
            ({fields}) VALUES ({values}) \
            ON CONFLICT ON CONSTRAINT pk_{table} \
            DO UPDATE SET {updates} \
            RETURNING *;"
        ).format(
            table=sql.Identifier(table),
            fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
            values=sql.SQL(", ").join(placeholders),
            updates=sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in cols
            )
        )

        try:
            conn = self.__conn_pool.getconn() 
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, processed_vals)
                row = cur.fetchone()
                conn.commit()
                logger.info(f"Inserted into {table}: {row}")
                return dict(row) if row is not None else {}
        finally:
            self.__conn_pool.putconn(conn)

    def _lookup(self, table: str, conditions: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Generic lookup helper that returns rows matching the conditions."""
        self._validate_table(table)

        if not conditions:
            raise ValueError("Conditions cannot be empty")

        where_clauses = []
        values = []
        for k, v in conditions.items():
            where_clauses.append(sql.SQL("{} = {}").format(sql.Identifier(k), sql.Placeholder()))
            values.append(v)

        query = sql.SQL(
            "SELECT * FROM {table} WHERE {where};"
        ).format(
            table=sql.Identifier(table),
            where=sql.SQL(" AND ").join(where_clauses),
        )

        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
                return [dict(row) for row in rows] if rows else []
        finally:
            self.__conn_pool.putconn(conn)

    def _update_trial_end(
        self, trial_name: str, death_timestamp: int, clean_exit: bool = True
    ) -> Dict[str, Any]:
        """Update an existing trial row with end-of-life fields.

        Returns the updated row as a dict. No-op (empty dict) if no row matched.
        """
        self._validate_table("trial")
        query = sql.SQL(
            """
            UPDATE {table}
            SET death_timestamp = %s,
                clean_exit = %s
            WHERE trial_name = %s
            RETURNING *;
            """
        ).format(table=sql.Identifier("trial"))

        conn = self.__conn_pool.getconn()
        try:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, [death_timestamp, clean_exit, trial_name])
                row = cur.fetchone()
                conn.commit()
                logger.info(f"Updated trial: {row}")
                return dict(row) if row is not None else {}
        finally:
            self.__conn_pool.putconn(conn)

    def insert_user(
        self,
        user_id: str,
        domain: str,
        email: Optional[str] = None,
        name: Optional[str] = None,
        created_by: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "user_id": user_id,
            "domain": domain,
            "email": email,
            "name": name,
        }
        if created_by:
            data["created_by_user_id"] = created_by[0]
            data["created_by_domain"] = created_by[1]
        else:
            data["created_by_user_id"] = "superadmin"
            data["created_by_domain"] = "superadmin"

        return self._upsert("user", data)

    def insert_project(
        self,
        project_id: Optional[str] = None,
        name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        created_by: Optional[Tuple[str, str]] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "project_id": project_id,
            "name": name,
            "details": details,
        }
        if created_by:
            data["created_by_user_id"] = created_by[0]
            data["created_by_domain"] = created_by[1]
        else:
            data["created_by_user_id"] = "superadmin"
            data["created_by_domain"] = "superadmin"

        return self._upsert("project", data)

    def insert_trial(
        self,
        trial_name: str,
        user_id: str,
        user_domain: str,
        birth_timestamp,
        project_id: Optional[str] = None,
        death_timestamp=None,
        clean_exit: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "trial_name": trial_name,
            "user_id": user_id,
            "user_domain": user_domain,
            "project_id": project_id,
            "birth_timestamp": birth_timestamp,
            "death_timestamp": death_timestamp,
            "clean_exit": clean_exit,
            "metadata": metadata,
        }
        return self._upsert("trial", data)

    def update_trial_end(
        self,
        trial_name: str,
        user_id: str,
        user_domain: str,
        death_timestamp=None,
        clean_exit: Optional[bool] = None,
        project_id: Optional[str] = None,
    ) -> Dict[str, Any]: ...

    def update_trial_project(
        self,
        trial_name: str,
        user_id: str,
        user_domain: str,
        project_id: Optional[str],
        time_start: int,
        time_end: int,
    ) -> Dict[str, Any]: ...

    def insert_user_project_role_linking(
        self,
        user_id: str,
        domain: str,
        project_id: str,
        role: str,
        id: Optional[str] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            # if `id` is None we omit it so DB default gen_random_uuid() applies
            "id": id,
            "user_id": user_id,
            "domain": domain,
            "project_id": project_id,
            "role": role,
        }
        return self._upsert("user_project_role_linking", data)

    def insert_graph_edge(
        self,
        source_trial_id: str,
        target_entity_id: str,
        target_entity_type: str,
        edge_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        data = {
            "edge_id": edge_id,
            "source_trial_id": source_trial_id,
            "target_entity_id": target_entity_id,
            "target_entity_type": target_entity_type,
        }
        return self._upsert("graph_edges", data)
