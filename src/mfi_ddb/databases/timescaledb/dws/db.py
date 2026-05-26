"""TimeScaleDB reader helper for DWS queries using a Threaded Connection Pool."""

from contextlib import contextmanager
import psycopg2
from psycopg2.pool import ThreadedConnectionPool


class TimeScaleReader:

    def __init__(self, dsn: dict, min_conn=5, max_conn=15):
        """Initialize a thread-safe connection pool for concurrent gRPC worker threads."""
        self.pool = ThreadedConnectionPool(min_conn, max_conn, **dsn)

    @contextmanager
    def _get_connection(self):
        """Context manager to safely borrow and return connections to the pool."""
        conn = self.pool.getconn()
        # Ensure changes aren't lingering in a transaction block
        conn.autocommit = True
        try:
            yield conn
        finally:
            self.pool.putconn(conn)

    def get_point(self, topic, ts, do_closest_past=True):
        """Return the nearest point at/around the requested timestamp."""
        op = "<=" if do_closest_past else ">="
        order = "DESC" if do_closest_past else "ASC"
        sql = f"""
        SELECT time, topic, component, metric, value_num, value_text, value_json
        FROM timeseries_data
        WHERE topic = %s AND time {op} %s
        ORDER BY time {order}
        LIMIT 1
        """
        # Safely acquire a thread-isolated connection from the pool
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (topic, ts))
                return cur.fetchone()

    def get_range(self, topic, start_time, end_time, page_size, page_token):
        """Return a page of points for the given topic/time range."""
        token_clause = ""
        params = [topic, start_time, end_time]
        if page_token:
            token_clause = "AND time > %s"
            params.append(page_token)
        params.append(page_size)

        sql = f"""
        SELECT time, topic, component, metric, value_num, value_text, value_json
        FROM timeseries_data
        WHERE topic = %s AND time >= %s AND time <= %s
        {token_clause}
        ORDER BY time ASC
        LIMIT %s
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchall()

    def stream_batch(self, topic, since_ts, limit):
        """Return the next batch of rows after the given timestamp."""
        sql = """
        SELECT time, topic, component, metric, value_num, value_text, value_json
        FROM timeseries_data
        WHERE topic = %s AND time > %s
        ORDER BY time ASC
        LIMIT %s
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (topic, since_ts, limit))
                return cur.fetchall()