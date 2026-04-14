"""
Reusable utilities for fetching data from metadata store tables.

Requires: psycopg2
    pip install psycopg2-binary
"""

from typing import Optional, Tuple, Dict, Any, List
import logging
import re
from datetime import datetime
import psycopg2.pool
from psycopg2 import sql
import psycopg2.extras
from app.services.pg_config import load_config

_ALLOWED_TABLES = {
    "user",
    "project",
    "trial",
    "user_project_role_linking",
    "graph_edges",
}

DEFAULT_USER = ("superadmin", "superadmin")

logger = logging.getLogger(__name__)

class MdsReader:
    def __init__(self):
        config = None
        try:
            config = load_config()
            self.__conn_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                **config
            )
            logger.info("Database connection pool created successfully")
        except Exception as error:
            logger.error(f"Error occurred while creating connection pool: {error}. Config used: {config}")
            raise error
        
    def __del__(self):
        if getattr(self, '_MdsReader__conn_pool', None):
            self.__conn_pool.closeall()
            logger.info("Database connection pool closed")

    def _validate_table(self, table: str):
        if table not in _ALLOWED_TABLES:
            raise ValueError(
                f"Table '{table}' is not allowed. Allowed: {sorted(_ALLOWED_TABLES)}"
            )

    def _lookup(self, table: str, conditions: Dict[str, Any]) -> Optional[List[Dict[str, Any]]]:
        """
        Generic lookup helper that returns rows matching the given conditions.

        This method builds a parameterized SQL SELECT query for a PostgreSQL table
        using the provided conditions. It supports both simple equality and a set
        of comparison operators.
        """
        self._validate_table(table)

        if not conditions:
            raise ValueError("Conditions cannot be empty")

        where_clauses = []
        values = []

        for k, v in conditions.items():
            col = sql.Identifier(k)

            if isinstance(v, tuple):
                op = v[0].lower()

                if op in (">", "<", ">=", "<="):
                    where_clauses.append(
                        sql.SQL("{} {} {}").format(
                            col,
                            sql.SQL(op),
                            sql.Placeholder()
                        )
                    )
                    values.append(v[1])

                elif op == "between":
                    if len(v) != 3:
                        raise ValueError(f"'between' requires exactly 2 values for {k}")
                    where_clauses.append(
                        sql.SQL("{} BETWEEN {} AND {}").format(
                            col,
                            sql.Placeholder(),
                            sql.Placeholder()
                        )
                    )
                    values.extend([v[1], v[2]])

                else:
                    raise ValueError(f"Unsupported operator '{op}' for column '{k}'")

            else:
                where_clauses.append(
                    sql.SQL("{} = {}").format(col, sql.Placeholder())
                )
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

    def get_trial_by_uuid(self, trial_uuid: str) -> Optional[Dict[str, Any]]:
        self._validate_table("trial")

        query = sql.SQL("SELECT * FROM {table} WHERE id = %s;").format(
            table=sql.Identifier("trial")
        )

        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (trial_uuid,))
                row = cur.fetchone()
                return dict(row) if row else None
        finally:
            self.__conn_pool.putconn(conn)

    def find_trials(
        self,
        enterprise_id: Optional[str] = None,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
        user_id: Optional[str] = None,
        user_domain: Optional[str] = None,
        site: Optional[str] = None,
        device: Optional[str] = None,
        trial_id: Optional[str] = None,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        search_terms: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        self._validate_table("trial")

        values: List[Any] = []
        where_clauses: List[sql.SQL] = []
        join_project = False

        if user_id is not None:
            where_clauses.append(sql.SQL("trial.user_id = %s"))
            values.append(user_id)

        if user_domain is not None:
            where_clauses.append(sql.SQL("trial.user_domain = %s"))
            values.append(user_domain)

        if project_id is not None:
            where_clauses.append(sql.SQL("trial.project_id = %s"))
            values.append(project_id)

        if trial_id is not None:
            where_clauses.append(sql.SQL("trial.trial_name = %s"))
            values.append(trial_id)

        if enterprise_id is not None:
            where_clauses.append(
                sql.SQL("(trial.metadata->>%s = %s OR trial.metadata->>%s = %s)")
            )
            values.extend(["enterprise", enterprise_id, "enterprise_id", enterprise_id])

        if site is not None:
            where_clauses.append(sql.SQL("trial.metadata->>%s = %s"))
            values.extend(["site", site])

        if device is not None:
            where_clauses.append(sql.SQL("trial.metadata->>%s = %s"))
            values.extend(["device", device])

        if project_name is not None:
            where_clauses.append(
                sql.SQL("(trial.metadata->>%s = %s OR project.name = %s)")
            )
            values.extend(["project_name", project_name, project_name])
            join_project = True

        if time_start is not None and time_end is not None:
            where_clauses.append(
                sql.SQL(
                    "(trial.birth_timestamp <= %s AND (trial.death_timestamp IS NULL OR trial.death_timestamp >= %s))"
                )
            )
            values.extend([time_end, time_start])
        elif time_start is not None:
            where_clauses.append(
                sql.SQL("(trial.death_timestamp IS NULL OR trial.death_timestamp >= %s)")
            )
            values.append(time_start)
        elif time_end is not None:
            where_clauses.append(sql.SQL("trial.birth_timestamp <= %s"))
            values.append(time_end)

        if search_terms is not None:
            for term in search_terms:
                term_like = f"%{term}%"
                where_clauses.append(
                    sql.SQL(
                        "(trial.trial_name ILIKE %s OR trial.metadata::text ILIKE %s)"
                    )
                )
                values.extend([term_like, term_like])

        trial_table = sql.Identifier("trial")
        query = sql.SQL("SELECT {trial}.* FROM {trial}").format(trial=trial_table)

        if join_project:
            query = query + sql.SQL(
                " LEFT JOIN {project} ON {trial}.project_id = {project}.project_id"
            ).format(
                project=sql.Identifier("project"),
                trial=trial_table,
            )

        if where_clauses:
            query = query + sql.SQL(" WHERE ") + sql.SQL(" AND ").join(where_clauses)

        query = query + sql.SQL(" ORDER BY trial.birth_timestamp DESC;")

        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
                return [dict(row) for row in rows] if rows else []
        finally:
            self.__conn_pool.putconn(conn)
