"""
KV database service.

Thin REST wrapper around the PostgreSQL `kv_store` table using the
connector functions in `databases.kv.connector`.

Run:

    uvicorn mfi_ddb.databases.kv_service:app --reload --port 8002

Environment:
    KVSTORE_PGHOST, KVSTORE_PGPORT,
    KVSTORE_PGUSER, KVSTORE_PGPASSWORD,
    KVSTORE_PGDATABASE
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from mfi_ddb.databases.kv import connector as kv_connector


class KVItem(BaseModel):
    id: str
    trial_id: str
    payload: Dict[str, Any]
    created_at: datetime


app = FastAPI(title="KV Service", version="0.1.0")


@app.get("/items", response_model=List[KVItem], summary="Retrieve KV items")
def get_items(trial_id: Optional[str] = Query(default=None)) -> List[KVItem]:
    try:
        rows = kv_connector.get_kv_items(trial_id=trial_id, limit=100)
        return [KVItem.model_validate(r) for r in rows]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health", summary="Health check")
def health() -> Dict[str, str]:
    try:
        kv_connector.get_kv_items(limit=1)
    except Exception as exc:  # pragma: no cover - simple health check
        raise HTTPException(status_code=503, detail=str(exc))
    return {"status": "ok"}


