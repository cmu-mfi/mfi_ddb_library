"""
Generic historian retrieval connector.

This module defines a very small abstraction over your actual historian
implementation (e.g. Aveva PI, OSIsoft PI Web API, Influx, etc.).

The Retrieval API calls :func:`query_historian_points` with a DSN and
expects back an iterable of (timestamp, value) pairs.

You are expected to replace the placeholder implementation with calls
into your real historian client library.
"""

from __future__ import annotations

from datetime import datetime
from typing import Iterable, List, Tuple


def query_historian_points(
    dsn: str, tag: str, start: datetime, end: datetime
) -> Iterable[Tuple[datetime, float]]:
    """
    Query time–series points from the historian.

    Parameters
    ----------
    dsn:
        Connection string / URL / configuration key understood by your
        historian client (see `RetrievalConfig.historian.dsn`).
    tag:
        Fully qualified historian tag / point name.
    start, end:
        Time range to query (inclusive).

    Returns
    -------
    Iterable[Tuple[datetime, float]]
        An iterable of `(timestamp, value)` pairs, ordered by timestamp.

    Notes
    -----
    Replace the `NotImplementedError` with your own implementation.
    """
    # Example skeleton:
    #
    #   client = YourHistorianClient.from_dsn(dsn)
    #   rows = client.query(tag=tag, start=start, end=end)
    #   for row in rows:
    #       yield row.timestamp, float(row.value)
    #
    raise NotImplementedError(
        "query_historian_points must be implemented for your historian backend"
    )


