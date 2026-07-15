"""
Reusable utilities for fetching data from metadata store tables.

Requires: psycopg2
    pip install psycopg2-binary
"""

import logging
import time  # For query latency measurements
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Iterable

import psycopg2.extras
import psycopg2.pool
from psycopg2 import sql

from app.services.pg_config import load_config

# ==============================================================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==============================================================================
from opentelemetry.metrics import get_meter, Observation

# Use the registered rws-app namespace
meter = get_meter("mfi.rws.app")

# Query Performance Metrics
db_query_duration = meter.create_histogram(
    "mfi_mds_db_query_duration_seconds",
    description="Time taken to execute SQL queries in the Metadata Store"
)

db_query_counter = meter.create_counter(
    "mfi_mds_db_queries_total",
    description="Total database queries executed by MdsReader"
)

# Reference placeholder for the connection pool to feed our async gauges
_pool_ref: Optional[psycopg2.pool.ThreadedConnectionPool] = None

def get_active_connections_count(options) -> Iterable[Observation]:
    if _pool_ref is not None:
        # Number of connections currently checked out by worker threads
        yield Observation(len(_pool_ref._used))
    else:
        yield Observation(0)

def get_idle_connections_count(options) -> Iterable[Observation]:
    if _pool_ref is not None:
        # Number of idle connections sitting waiting in the pool
        yield Observation(len(_pool_ref._pool))
    else:
        yield Observation(0)

# Register Dynamic Connection Pool Gauges
meter.create_observable_gauge(
    "mfi_mds_db_connections_active",
    callbacks=[get_active_connections_count],
    description="Number of metadata database connections currently active"
)

meter.create_observable_gauge(
    "mfi_mds_db_connections_idle",
    callbacks=[get_idle_connections_count],
    description="Number of idle metadata database connections available in the pool"
)
# ==============================================================================

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
    def __init__(self, config_file = 'pg_database.ini'):
        global _pool_ref
        config = load_config(filename=config_file)
        try:
            self.__conn_pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=1,
                maxconn=10,
                **config
            )
            # Store pool reference globally for the async gauges to monitor
            _pool_ref = self.__conn_pool
            logger.info("Database connection pool created successfully")
        except Exception as error:
            logger.error(f"Error occurred while creating connection pool: {error}. Config used: {config}")
            raise error
        
    def __del__(self):
        global _pool_ref
        if getattr(self, '_MdsReader__conn_pool', None):
            self.__conn_pool.closeall()
            _pool_ref = None
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
        start_time = time.perf_counter()
        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
                
                # Record successful database lookup
                duration = time.perf_counter() - start_time
                db_query_duration.record(duration, {"query_type": f"lookup_{table}", "status": "SUCCESS"})
                db_query_counter.add(1, {"query_type": f"lookup_{table}", "status": "SUCCESS"})
                
                return [dict(row) for row in rows] if rows else []
        except Exception as e:
            # Record failed database lookup
            duration = time.perf_counter() - start_time
            db_query_duration.record(duration, {"query_type": f"lookup_{table}", "status": "ERROR"})
            db_query_counter.add(1, {"query_type": f"lookup_{table}", "status": "ERROR"})
            raise e
        finally:
            if conn is not None:
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
        start_time = time.perf_counter()
        try:
            conn = self.__conn_pool.getconn()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, values)
                rows = cur.fetchall()
                rows = self._validate_access_and_filter_trial_rows(rows, (user_id,user_domain))
                
                # Record successful search trials duration
                duration = time.perf_counter() - start_time
                db_query_duration.record(duration, {"query_type": "find_trials", "status": "SUCCESS"})
                db_query_counter.add(1, {"query_type": "find_trials", "status": "SUCCESS"})
                
                return [dict(row) for row in rows] if rows else []
        except Exception as e:
            # Record failed search trials duration
            duration = time.perf_counter() - start_time
            db_query_duration.record(duration, {"query_type": "find_trials", "status": "ERROR"})
            db_query_counter.add(1, {"query_type": "find_trials", "status": "ERROR"})
            raise e
        finally:
            if conn is not None:
                self.__conn_pool.putconn(conn)