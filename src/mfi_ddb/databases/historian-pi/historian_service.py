"""
Historian (time-series) service.

Thin REST wrapper around whatever historian backend you configure,
using the `query_historian_points` function from
`databases.historian.connector`.

Run:

    uvicorn mfi_ddb.databases.historian_service:app --reload --port 8004

Environment:
    HISTORIAN_DSN  (connection string / URL understood by your historian client)
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Dict, List

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from mfi_ddb.databases.historian.connector import query_historian_points


class HistorianPoint(BaseModel):
    timestamp: datetime
    value: float


app = FastAPI(title="Historian Service", version="0.1.0")


def _dsn() -> str:
    dsn = os.environ.get("HISTORIAN_DSN")
    if not dsn:
        raise RuntimeError("HISTORIAN_DSN environment variable is not set")
    return dsn


@app.get(
    "/query",
    response_model=List[HistorianPoint],
    summary="Query historian time-series data",
)
def query(
    tag: str = Query(..., description="Historian tag / PI point name"),
    start: datetime = Query(..., description="Start time (ISO-8601)"),
    end: datetime = Query(..., description="End time (ISO-8601)"),
) -> List[HistorianPoint]:
    try:
        rows = list(query_historian_points(_dsn(), tag=tag, start=start, end=end))
        return [HistorianPoint(timestamp=ts, value=val) for ts, val in rows]
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health", summary="Health check")
def health() -> Dict[str, str]:
    # We only check that DSN is configured; a deeper check would query a
    # trivial time range for a known tag.
    try:
        _ = _dsn()
    except Exception as exc:  # pragma: no cover - simple health check
        raise HTTPException(status_code=503, detail=str(exc))
    return {"status": "ok"}


