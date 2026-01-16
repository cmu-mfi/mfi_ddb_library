"""
Reusable utilities for inserting rows into the metadata store tables.

Design considerations:
- Safe SQL composition (psycopg2.sql) to avoid injection via table/column names
- Generic `_insert` helper that omits columns when value is None so DB defaults apply
- Per-table convenience wrappers that map higher-level arguments into columns

Usage example:
    from mfi_ddb.scripts import metadata_utils as mu
    row = mu.insert_user_detail('alice', 'example.com', email='a@x.com')
    print(row)

Requires: psycopg2
    pip install psycopg2-binary
"""

from typing import Optional, Tuple, Dict, Any, List
import os
import psycopg2
from psycopg2 import sql
import psycopg2.extras

_ALLOWED_TABLES = {
    "user_detail",
    "project_detail",
    "trial_detail",
    "user_project_role_linking",
    "graph_edges",
}


def get_conn_from_env():
    params = {
        "host": os.environ.get("METADASTORE_PGHOST"),
        "port": int(os.environ.get("METADASTORE_PGPORT")),
        "user": os.environ.get("METADASTORE_PGUSER"),
        "password": os.environ.get("METADASTORE_PGPASSWORD"),
        "dbname": os.environ.get("METADASTORE_PGDATABASE"),
    }
    return psycopg2.connect(**params)


def _validate_table(table: str):
    if table not in _ALLOWED_TABLES:
        raise ValueError(f"Table '{table}' is not allowed. Allowed: {sorted(_ALLOWED_TABLES)}")


def _insert(table: str, values: Dict[str, Any]) -> Dict[str, Any]:
    """Generic insert helper.

    - Omits keys with value is None so DB defaults (e.g., DEFAULT gen_random_uuid()) apply.
    - Uses psycopg2.sql to safely compose identifiers.
    - Returns the inserted row as a dict.
    """
    _validate_table(table)

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

    query = sql.SQL("INSERT INTO {table} ({fields}) VALUES ({values}) RETURNING *;").format(
        table=sql.Identifier(table),
        fields=sql.SQL(", ").join(map(sql.Identifier, cols)),
        values=sql.SQL(", ").join(placeholders),
    )

    conn = get_conn_from_env()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, processed_vals)
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row is not None else {}
    finally:
        conn.close()


def _update_trial_end(trial_name: str,
                      death_timestamp: int,
                      clean_exit: bool = True) -> Dict[str, Any]:
    """Update an existing trial_detail row with end-of-life fields.

    Returns the updated row as a dict. No-op (empty dict) if no row matched.
    """
    _validate_table("trial_detail")
    query = sql.SQL(
        """
        UPDATE {table}
        SET death_timestamp = %s,
            clean_exit = %s
        WHERE trial_name = %s
        RETURNING *;
        """
    ).format(table=sql.Identifier("trial_detail"))

    conn = get_conn_from_env()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, [death_timestamp, clean_exit, trial_name])
            row = cur.fetchone()
            conn.commit()
            return dict(row) if row is not None else {}
    finally:
        conn.close()


def insert_user_detail(user_id: str,
                       domain: str,
                       email: Optional[str] = None,
                       name: Optional[str] = None,
                       created_by: Optional[Tuple[str, str]] = None) -> Dict[str, Any]:
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

    return _insert("user_detail", data)


def insert_project_detail(project_id: Optional[str] = None,
                          name: Optional[str] = None,
                          details: Optional[Dict[str, Any]] = None,
                          created_by: Optional[Tuple[str, str]] = None) -> Dict[str, Any]:
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

    return _insert("project_detail", data)


def insert_trial_detail(trial_name: str,
                        user_id: str,
                        user_domain: str,
                        project_id: Optional[str],
                        birth_timestamp,
                        death_timestamp=None,
                        clean_exit: Optional[bool] = None,
                        metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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
    return _insert("trial_detail", data)


def update_trial_detail_end(trial_name: str,
                            death_timestamp: int,
                            clean_exit: bool = True) -> Dict[str, Any]:
    """Public wrapper to update trial end fields.

    Example:
        update_trial_detail_end("AIDF_FINAL_DEMO", 1700000000, True)
    """
    return _update_trial_end(trial_name, death_timestamp, clean_exit)


def insert_user_project_role_linking(user_id: str,
                                    domain: str,
                                    project_id: str,
                                    role: str,
                                    id: Optional[str] = None) -> Dict[str, Any]:
    data: Dict[str, Any] = {
        # if `id` is None we omit it so DB default gen_random_uuid() applies
        "id": id,
        "user_id": user_id,
        "domain": domain,
        "project_id": project_id,
        "role": role,
    }
    return _insert("user_project_role_linking", data)


def insert_graph_edge(source_trial_id: str,
                      target_entity_id: str,
                      target_entity_type: str,
                      edge_id: Optional[str] = None) -> Dict[str, Any]:
    data = {
        "edge_id": edge_id,
        "source_trial_id": source_trial_id,
        "target_entity_id": target_entity_id,
        "target_entity_type": target_entity_type,
    }
    return _insert("graph_edges", data)
