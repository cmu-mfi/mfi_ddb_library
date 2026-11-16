"""
Reusable utilities for fetching data from metadata store tables.

Requires: psycopg2
    pip install psycopg2-binary
"""

from typing import Optional, Tuple, Dict, Any, List
import logging
from datetime import datetime
import psycopg2.pool
from psycopg2 import sql
import psycopg2.extras
from app.services.pg_config import load_config

_ALLOWED_TABLES = {
    "user",
    "ddb_user",
    "project",
    "trial",
    "user_project_role_linking",
    "graph_edges",
}

DEFAULT_USER = ("superadmin", "superadmin")

# Platform accounts that are not people; hidden from user search results.
SYSTEM_USERS = {DEFAULT_USER}

logger = logging.getLogger(__name__)

class MdsReader:
    def __init__(self, config_file = 'pg_database.ini'):
        config = load_config(filename=config_file)
        try:
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

        conn = None
        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
                return [dict(row) for row in rows] if rows else []
        finally:
            self.__conn_pool.putconn(conn)

    def _validate_access_and_filter_trial_rows(self, trial_rows: Optional[List], user: Tuple[str,Optional[str]]) -> List:
        filtered_rows = []
        if trial_rows is None:
            return []
        
        if user == ("superadmin", "superadmin"):
            return trial_rows
        
        for row in trial_rows:
            if user == (row["user_id"], row["user_domain"]):
                filtered_rows.append(row)
                continue
            if row["project_id"] is None:
                continue
            user_roles = self._lookup("user_project_role_linking", {
                "project_id":row["project_id"],
                "user_id":user[0],
                "domain":user[1]
            })
            if user_roles is None or len(user_roles)==0:
                continue
            if len(user_roles) > 1:
                logger.warning(f"Same user {user} has multiple roles for project {row['project_id']}.")

            roles = set([u["role"] for u in user_roles])
            if roles & {"admin", "maintainer"}:
                filtered_rows.append(row)
        
        return filtered_rows           
            

    # ------------------------------------------------------------------
    # Users (directory lookups). Only the public fields a "pick a person"
    # list needs are returned - never email or audit columns.
    # ------------------------------------------------------------------
    _USER_FIELDS = sql.SQL("user_id, domain, name")

    def get_user(self, user_id: str, domain: str) -> Optional[Dict[str, Any]]:
        """Return {user_id, domain, name} for one registered user, or None."""
        query = sql.SQL("SELECT {fields} FROM ddb_user WHERE user_id = %s AND domain = %s;").format(
            fields=self._USER_FIELDS
        )
        rows = self._fetch(query, [user_id, domain])
        return rows[0] if rows else None

    def search_users(
        self, search: Optional[str] = None, limit: int = 25, include_system: bool = False
    ) -> List[Dict[str, Any]]:
        """Registered users whose id, name or domain contains `search` (case-insensitive).

        System accounts (SYSTEM_USERS, e.g. superadmin) are left out unless
        include_system is set: they are not people to pick in a list.
        """
        values: List[Any] = []
        clauses: List[sql.Composable] = []
        if search:
            like = f"%{search.strip()}%"
            clauses.append(sql.SQL("(user_id ILIKE %s OR name ILIKE %s OR domain ILIKE %s)"))
            values.extend([like, like, like])
        if not include_system:
            for user_id, domain in SYSTEM_USERS:
                clauses.append(sql.SQL("NOT (user_id = %s AND domain = %s)"))
                values.extend([user_id, domain])
        where = sql.SQL(" WHERE ") + sql.SQL(" AND ").join(clauses) if clauses else sql.SQL("")
        query = (
            sql.SQL("SELECT {fields} FROM ddb_user").format(fields=self._USER_FIELDS)
            + where
            + sql.SQL(" ORDER BY lower(user_id), domain LIMIT %s;")
        )
        values.append(limit)
        return self._fetch(query, values)

    def _fetch(self, query, values) -> List[Dict[str, Any]]:
        conn = None
        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                return [dict(row) for row in cur.fetchall()]
        finally:
            if conn is not None:
                self.__conn_pool.putconn(conn)

    def get_trial_by_uuid(self, trial_uuid: str, user_id: str, user_domain: Optional[str]) -> Optional[Dict[str, Any]]:
        row = self._lookup("trial", {"uuid": trial_uuid})
        if row is None:
            return None
        row = self._validate_access_and_filter_trial_rows(row, (user_id,user_domain))        
        return row[0] if row else None

    def find_trials(
        self,
        user_id: str,        
        user_domain: Optional[str] = None,
        enterprise_id: Optional[str] = None,
        time_start: Optional[datetime] = None,
        time_end: Optional[datetime] = None,
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

        if project_id is not None:
            where_clauses.append(sql.SQL("trial.project_id = %s"))
            values.append(project_id)

        if trial_id is not None:
            where_clauses.append(sql.SQL("trial.trial_name = %s"))
            values.append(trial_id)

        '''
        # SEARCHING METADATA FOR SPECIFIC KEY VALUES IS UNIMPLEMENTED
        # IDEAS: 
        # * Do RAG-like search?
        # * Use pg_vector?
        
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
        '''
        if project_name is not None:
            where_clauses.append(sql.SQL("(project.project_name = %s)"))
            values.append(project_name)
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

        conn = None
        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
                rows = self._validate_access_and_filter_trial_rows(rows, (user_id,user_domain))
                return [dict(row) for row in rows] if rows else []
        finally:
            if conn is not None:
                self.__conn_pool.putconn(conn)
