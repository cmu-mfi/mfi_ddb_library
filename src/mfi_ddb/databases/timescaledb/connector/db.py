"""TimeScaleDB writer helper for batch inserts. Batch inserts are more efficient than single-row inserts for high-throughput data."""

import psycopg2
from psycopg2.extras import execute_batch

class TimeScaleWriter:
    def __init__(self, dsn: dict):
        """Open a DB connection using the provided DSN dict."""
        self.conn = psycopg2.connect(**dsn)
        self.conn.autocommit = True

    def insert_rows(self, rows):
        """Batch insert time-series rows into the timeseries_data table."""
        # Rows shape: (time, topic, component, metric, value_num, value_text, value_json)
        sql = """
        INSERT INTO timeseries_data
        (time, topic, component, metric, value_num, value_text, value_json)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        with self.conn.cursor() as cur:
            execute_batch(cur, sql, rows, page_size=500)