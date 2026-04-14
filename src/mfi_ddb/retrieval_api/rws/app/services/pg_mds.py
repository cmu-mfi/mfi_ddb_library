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
from pg_config import load_config

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
        try:
            config = load_config()
            self.__conn_pool = psycopg2.pool.ThreadedConnectionPool(
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
        
    def __del__(self):
        if self.__conn_pool:
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

        Args:
            table (str):
                Name of the table to query. Must pass internal validation.

            conditions (Dict[str, Any]):
                Mapping of column names to condition values. Each value can be:

                1. Direct value (equality):
                    {"status": "active"}
                    → WHERE status = %s

                2. Tuple with comparison operator:
                    {
                        "age": (">", 18),
                        "score": ("<=", 100)
                    }
                    → WHERE age > %s AND score <= %s

                    Supported operators:
                        ">", "<", ">=", "<="

                3. Between range (inclusive):
                    {
                        "timestamp": ("between", start_time, end_time)
                    }
                    → WHERE timestamp BETWEEN %s AND %s

                    Note: BETWEEN is inclusive:
                        start_time <= column <= end_time

        Returns:
            List[Dict[str, Any]]:
                A list of rows as dictionaries. Returns an empty list if no rows match.

        Raises:
            ValueError:
                - If conditions is empty
                - If an unsupported operator is provided
                - If a "between" condition does not include exactly two values

        Notes:
            - All queries are safely parameterized using psycopg2 to prevent SQL injection.
            - Column names are safely escaped using psycopg2.sql.Identifier.
            - For time-based queries, consider using:
                (">=", start) and ("<", end)
            instead of "between" to avoid overlapping intervals.
        """
        self._validate_table(table)

        if not conditions:
            raise ValueError("Conditions cannot be empty")

        where_clauses = []
        values = []

        for k, v in conditions.items():
            col = sql.Identifier(k)

            # Operator-based conditions
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

            # Default equality
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