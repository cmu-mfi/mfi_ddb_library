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

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import psycopg2.extras
import psycopg2.pool
from pg_config import load_config
from psycopg2 import sql

_ALLOWED_TABLES = {
    "ddb_user",
    "project",
    "trial",
    "user_project_role_linking",
    "graph_edges",
}

DEFAULT_USER = ("superadmin", "superadmin")

logger = logging.getLogger(__name__)

class MdsConnector:
    def __init__(self, config_path='pg_database.ini'):
        config = load_config(filename=config_path)
        try:
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

    def _upsert(self, table: str, values: Dict[str, Any]) -> Dict[str, Any]:
        """Generic upsert helper.

        - Omits keys with value is None so DB defaults (e.g., DEFAULT gen_random_uuid()) apply.
        - Uses psycopg2.sql to safely compose identifiers.
        - Returns the upserted row as a dict.
        """
        self._validate_table(table)

        # Prefer explicit timestamp for created_at/updated_at when provided.
        # "timestamp" is not a real table column and must not be inserted directly.
        timestamp_value = values.pop("timestamp", None)
        if isinstance(timestamp_value, str):
            try:
                timestamp_value = datetime.fromisoformat(timestamp_value)
            except ValueError:
                # If not ISO-parsable, keep as raw string and let psycopg2/Postgres attempt conversion.
                pass

        # Filter out None values so DB defaults apply
        cols = [k for k, v in values.items() if v is not None]
        vals = [values[k] for k in cols]

        if not cols and timestamp_value is None:
            raise ValueError("No columns to insert")

        # Convert python dict/list to JSON wrapper for psycopg2
        processed_vals: List[Any] = []
        for v in vals:
            if isinstance(v, dict):
                processed_vals.append(psycopg2.extras.Json(v))  # JSONB
            else:
                processed_vals.append(v)

        placeholders = [sql.Placeholder() for _ in cols]

        # created_at and updated_at may use explicit timestamp value or CURRENT_TIMESTAMP otherwise.
        created_at_clause = sql.Placeholder() if timestamp_value is not None else sql.SQL("CURRENT_TIMESTAMP")
        updated_at_clause = sql.Placeholder() if timestamp_value is not None else sql.SQL("CURRENT_TIMESTAMP")
        if timestamp_value is not None:
            processed_vals.append(timestamp_value)
            processed_vals.append(timestamp_value)

        # constrain_name = f"pk_{table}"
        query = sql.SQL(
            "INSERT INTO {table} \
            ({fields}, created_at) VALUES ({values}, {created_at}) \
            ON CONFLICT ON CONSTRAINT {constraint} \
            DO UPDATE SET {updates}, updated_at = {updated_at} \
            RETURNING *;"
        ).format(
            table=sql.Identifier(table),
            constraint=sql.Identifier(f"pk_{table}"),
            fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
            values=sql.SQL(", ").join(placeholders),
            created_at=created_at_clause,
            updated_at=updated_at_clause,
            updates=sql.SQL(", ").join(
                sql.SQL("{} = EXCLUDED.{}").format(sql.Identifier(c), sql.Identifier(c)) for c in cols
            ),
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

    def insert_user(
        self,
        user: Tuple[str, str],
        timestamp: str,
        created_by: Optional[Tuple[str, str]] = None,
        email: Optional[str] = None,
        name: Optional[str] = None,        
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "user_id": user[0],
            "domain": user[1],
            "created_by_user_id": created_by[0] if created_by else DEFAULT_USER[0],
            "created_by_domain": created_by[1] if created_by else DEFAULT_USER[1],
            "email": email,
            "name": name,
            "timestamp": timestamp,
        }
        try:
            return self._upsert("ddb_user", data)
        except Exception as e:
            logger.error(f"Error inserting user {user}: {e}")
            return {}

    def insert_project(
        self,
        timestamp: str,
        project_id: Optional[str] = None,
        project_name: Optional[str] = None,
        created_by: Optional[Tuple[str, str]] = DEFAULT_USER,
        details: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:        
        if project_id is not None:
            condition = {"project_id": project_id}
        else:
            condition = {"project_name": project_name}
        
        project_row = self._lookup("project", condition)
        
        if project_row is not None:
            if len(project_row)>1:
                # TODO: IT IS A MESS IF THERE ARE MULTIPLE RECORDS
                # TRY AND USE USER PROJECT ROLES TO IDENTIFY WHICH
                # PROJECT THE USER IS TRYING TO MODIFY
                ...
            elif len(project_row)==1:
                # CHECK IF USER IS ALLOWED TO UPDATE
                if created_by is not DEFAULT_USER:
                    user_role = self._lookup(
                        "user_project_role_linking",
                        {
                            "user_id": created_by[0],
                            "domain": created_by[1],
                            "project_id": project_row[0]["project_id"]
                        }
                    )
                    if user_role is None or len(user_role) == 0:
                        logging.warning(f"User {created_by} not allowed to update project {project_row[0]['project_name']}")
                        return {}
                    if user_role[0]["role"] not in ["admin", "maintainer"]:
                        logging.warning(f"User {created_by} has role {user_role[0]['role']}")
                        logging.warning(f"User {created_by} not allowed to update project {project_row[0]['project_name']}")
                        return {}
                    
                # UPDATE VALUES
                project_id = project_row[0]["project_id"]
                old_details = project_row[0]["details"]
                old_details.update(details)
                details = old_details
                if created_by is None:
                    created_by = (project_row[0]["created_by_user_id"],
                                  project_row[0]["created_by_domain"])
                
        data: Dict[str, Any] = {
            "project_id": project_id,
            "project_name": project_name,
            "created_by_user_id": created_by[0],
            "created_by_domain": created_by[1],
            "details": details,
            "timestamp": timestamp,
        }        
        try:
            return self._upsert("project", data)
        except Exception as e:
            logger.error(f"Error inserting project {project_id}: {e}")
            return {}

    def insert_trial(
        self,
        trial_id: str,
        user: Tuple[str, str],
        birth_timestamp: str,
        project_id: Optional[str] = None,
        death_timestamp: Optional[str] = None,
        clean_exit: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
        data_topics: Optional[List[str]] = None,
        timestamp: Optional[str] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "trial_name": trial_id,
            "user_id": user[0],
            "user_domain": user[1],
            "project_id": project_id,
            "birth_timestamp": birth_timestamp,
            "death_timestamp": death_timestamp if death_timestamp is not None else birth_timestamp,
            "clean_exit": clean_exit,
            "metadata": metadata,
            "data_topics": data_topics,
            "timestamp": timestamp,
        }
        try:
            return self._upsert("trial", data)
        except Exception as e:
            logger.error(f"Error inserting trial {trial_id}: {e}")
            return {}

    def update_trial(
        self,
        trial_id: str,
        user: Tuple[str, str],
        birth_timestamp: Optional[str] = None,
        project_id: Optional[str] = None,
        death_timestamp: Optional[str] = None,
        clean_exit: Optional[bool] = None,
        metadata: Optional[Dict[str, Any]] = None,
        data_topics: Optional[List[str]] = None,
        timestamp: Optional[str] = None,
        time_start: Optional[str] = '-infinity',
        time_end: Optional[str] = None,
    ) -> Dict[str, Any]:
        data: Dict[str, Any] = {
            "trial_name": trial_id,
            "user_id": user[0],
            "user_domain": user[1],
            "project_id": project_id,
            "birth_timestamp": birth_timestamp,
            "death_timestamp": death_timestamp,
            "clean_exit": clean_exit,
            "metadata": metadata,
            "timestamp": timestamp,
            "data_topics": data_topics,
        }

        time_end = datetime.now(timezone.utc).isoformat() if time_end is None else time_end
        
        selected_trials = self._lookup("trial", {
            "trial_name": trial_id, 
            "user_id": user[0], 
            "user_domain": user[1],
            "birth_timestamp": ('between', time_start, time_end) if time_start and time_end else None,
            "death_timestamp": ('between', time_start, time_end) if time_start and time_end else None
        })
                
        if not selected_trials:
            logger.warning(f"No trial found for update with trial_id={trial_id}, user={user}, time range=({time_start}, {time_end}). Skipping update.")
            return {}
        elif len(selected_trials) == 0:
            logger.warning(f"No trial found for update with trial_id={trial_id}, user={user}, time range=({time_start}, {time_end}). Skipping update.")
            return {}
        elif len(selected_trials) > 1:
            logger.warning(f"Multiple trials found for update with trial_id={trial_id}, user={user}, time range=({time_start}, {time_end}). \
                This should not happen. Update may affect multiple rows. Skipping update.")
            return {}
        
        # Use the primary key 'id' for the upsert to target the correct row
        data["uuid"] = selected_trials[0]["uuid"]
        
        try:
            return self._upsert("trial", data)
        except Exception as e:
            logger.error(f"Error inserting trial {trial_id}: {e}")
            return {}

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
        try:
            return self._upsert("user_project_role_linking", data)
        except Exception as e:
            logger.error(f"Error inserting user-project-role-linking: {e}")
            return {}            

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
        # return self._upsert("graph_edges", data)
        logger.warning("NOT IMPLEMENTED AND TESTED")
        return {}
