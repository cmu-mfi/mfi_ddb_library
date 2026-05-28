"""TimeScaleDB reader helper for DWS queries using a Threaded Connection Pool."""

from contextlib import contextmanager
import re
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from typing import Any


class TimeScaleReader:

    def __init__(self, dsn: dict, min_conn=5, max_conn=15):
        """Initialize a thread-safe connection pool for concurrent gRPC worker threads."""
        # Use a flexible annotation for `pool` so unit-test harnesses can
        # replace it with test doubles (CapturingPool) without causing
        # static type-checker warnings from Pylance. At runtime this will
        # still be a ThreadedConnectionPool in production.
        self.pool: Any = ThreadedConnectionPool(min_conn, max_conn, **dsn)

    @staticmethod
    def _mqtt_topic_to_regex(topic_pattern: str) -> str:
        """Translate an MQTT topic filter into a full-match PostgreSQL regex.

        MQTT wildcards are intentionally mapped with their topic-level semantics:
        '+' matches exactly one topic level and '#' matches zero or more levels.
        Any literal characters are escaped so topic text is not interpreted as
        regex syntax.
        """
        regex_parts = []
        # Characters that must be escaped when inserted into a regex outside
        # of a character class. Note: hyphen (`-`) is safe outside of classes
        # so we intentionally do not escape it to keep patterns readable and
        # aligned with existing tests/expectations.
        regex_meta = set('.^$*?{}[]\\|()')
        for ch in topic_pattern:
            if ch == "+":
                regex_parts.append(r"[^/]+")
            elif ch == "#":
                regex_parts.append(r".*")
            else:
                if ch in regex_meta:
                    regex_parts.append("\\" + ch)
                else:
                    regex_parts.append(ch)
        return "^" + "".join(regex_parts) + "$"

    @staticmethod
    def _topic_clause(topic_pattern: str):
        """Return the SQL topic filter fragment and bound parameter.

        Exact topics keep the fast equality predicate. MQTT wildcard filters are
        expanded into a regex match so callers can use the same query surface
        for exact topics and pattern matches.
        """
        if "+" not in topic_pattern and "#" not in topic_pattern:
            return "topic = %s", topic_pattern
        return "topic ~ %s", TimeScaleReader._mqtt_topic_to_regex(topic_pattern)

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
        """Return the nearest point on the requested side of the timestamp.

        The query is deliberately one-sided:
        - do_closest_past=True means "give me the newest row at or before ts".
        - do_closest_past=False means "give me the oldest row at or after ts".

        This method does not fall back to the opposite side. If no row exists on
        the requested side, the caller receives no datapoint.
        """
        topic_clause, topic_param = self._topic_clause(topic)
        op = "<=" if do_closest_past else ">="
        order = "DESC" if do_closest_past else "ASC"
        sql = f"""
        SELECT time, topic, component, metric, value_num, value_text, value_json
        FROM timeseries_data
        WHERE {topic_clause} AND time {op} %s
        ORDER BY time {order}
        LIMIT 1
        """
        # Safely acquire a thread-isolated connection from the pool
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (topic_param, ts))
                return cur.fetchone()

    def get_range(self, topic, start_time, end_time, page_size, page_token):
        """Return a page of points for the given topic/time range.

        Wildcard topic filters are expanded here as well so callers can page over
        a whole family of topics. Ordering stays time-first to preserve the same
        cursor shape used by the existing API.
        """
        topic_clause, topic_param = self._topic_clause(topic)
        token_clause = ""
        params = [topic_param, start_time, end_time]
        if page_token:
            token_clause = "AND time > %s"
            params.append(page_token)
        params.append(page_size)

        sql = f"""
        SELECT time, topic, component, metric, value_num, value_text, value_json
        FROM timeseries_data
        WHERE {topic_clause} AND time >= %s AND time <= %s
        {token_clause}
        ORDER BY time ASC
        LIMIT %s
        """
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, tuple(params))
                return cur.fetchall()

    def stream_batch(self, topic, since_ts, limit):
        """Return the next batch of rows after the given timestamp.

        The streaming path uses the same wildcard topic filtering as the point
        and range queries so a single subscription can tail a whole topic family.
        """
        topic_clause, topic_param = self._topic_clause(topic)
        sql = """
        SELECT time, topic, component, metric, value_num, value_text, value_json
        FROM timeseries_data
        WHERE {topic_clause} AND time > %s
        ORDER BY time ASC
        LIMIT %s
        """.format(topic_clause=topic_clause)
        with self._get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, (topic_param, since_ts, limit))
                return cur.fetchall()