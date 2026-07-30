"""TimeScaleDB writer helper for batch inserts. Batch inserts are more efficient than single-row inserts for high-throughput data."""

import psycopg2
from psycopg2.extras import execute_batch
from typing import Optional, Any

# Define the schema initialization script as a constant
INIT_SCHEMA_SQL = """
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS timeseries_data (
  time        TIMESTAMPTZ NOT NULL,
  topic       TEXT NOT NULL,
  component   TEXT NOT NULL,
  metric      TEXT NOT NULL,
  value_num   DOUBLE PRECISION,
  value_text  TEXT,
  value_json  JSONB
);

-- Note: create_hypertable fails if called on an existing hypertable.
-- Using an anonymous DO block wraps it safely for repeated runs.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM _timescaledb_catalog.hypertable WHERE table_name = 'timeseries_data'
    ) THEN
        PERFORM create_hypertable('timeseries_data', 'time');
    END IF;
END $$;
"""

import time
from opentelemetry.metrics import get_meter

meter = get_meter("mfi.data_adapter.app")

tsdb_insert_executed_at = meter.create_gauge(
    "mfi_timescale_last_insert_executed_at_seconds",
    description="Unix timestamp in seconds when the batch insertion was executed",
    unit="s"
)


class TimeScaleWriter:
    def __init__(self, db_config: dict):
        """Store config and open a DB connection lazily via `connect()`.

        Tests and import-time code should avoid attempting a socket open;
        constructing this object will call `connect()` but callers can choose
        to defer creating the instance if they wish.
        """
        self.db_config = db_config
        # Use a loose Any typing so test doubles (mocks) can assign a fake
        # connection object without static type errors from Pylance.
        self.conn: Optional[Any] = None
        self.connect()

    def connect(self):
        """Initializes the database connection socket."""
        # Use a local variable so static checkers know this is a concrete
        # connection object before assigning back to the Optional field.
        conn = psycopg2.connect(**self.db_config)
        # Keep autocommit so we don't need explicit transaction blocks here
        conn.autocommit = True
        self.conn = conn

        # Execute schema migration safely right after connection established
        self._initialize_schema()
    
    def _initialize_schema(self):
        """Ensures extension, tables, and hypertables exist on startup."""
        if self.conn is None:
            return
        try:
            with self.conn.cursor() as cur:
                cur.execute(INIT_SCHEMA_SQL)
                print("TimescaleDB schema successfully verified/initialized.")
        except Exception as e:
            print(f"CRITICAL: Failed to initialize schema on startup: {e}")
            raise e
        
    def reconnect(self):
        """Safely tear down and reconstruct the connection state."""
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            # Best-effort close; ignore errors and re-establish below
            pass
        self.connect()

    def is_closed(self) -> bool:
        """Explicit check for psycopg2 connection status integer flags.

        psycopg2 exposes a numeric `closed` attribute: 0 == open, non-zero == closed.
        """
        return getattr(self.conn, "closed", 1) != 0

    def insert_rows(self, rows):
        """Batch insert time-series rows into the timeseries_data table."""
        # Rows shape: (time, topic, component, metric, value_num, value_text, value_json)
        sql = """
        INSERT INTO timeseries_data
        (time, topic, component, metric, value_num, value_text, value_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        conn = self.conn
        if conn is None:
            raise RuntimeError("Database connection is not initialized")

        with conn.cursor() as cur:
            execute_batch(cur, sql, rows, page_size=500)

        # Record exact completion time (float with microsecond/millisecond precision)
        tsdb_insert_executed_at.set(
            time.time(),
            {"status": "SUCCESS"}
        )