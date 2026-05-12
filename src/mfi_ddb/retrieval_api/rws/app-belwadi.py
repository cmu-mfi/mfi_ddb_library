"""
Lightweight Retrieval API service.

This module exposes REST endpoints for each backing data store:

- /metadata/...  -> PostgreSQL metadata store
- /kv/...        -> PostgreSQL key–value store
- /historian/... -> External historian (e.g. Aveva PI) – queried via a connector
- /cfs/...       -> Cloud file storage backed by local/remote files

The goal is to keep this API thin, declarative, and easy to extend.

Usage (development):
    uvicorn mfi_ddb.retrieval_api.app:app --reload --port 8000

Configuration:
    The API reads database and storage connection info from a simple
    YAML/JSON file whose path is given by the MFI_RETRIEVAL_CONFIG
    environment variable. See `config/databases.example.yaml`.
"""

from __future__ import annotations

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

import httpx
import yaml
from fastapi import Depends, FastAPI, HTTPException, Query
from pydantic import BaseModel


###############################################################################
# Configuration helpers
###############################################################################


class ServiceConfig(BaseModel):
    """
    Configuration for a backing database service.

    Each service is a small FastAPI (or similar) app that exposes REST
    endpoints local to that database. The Retrieval API talks to these
    services over HTTP only.
    """

    base_url: str  # e.g. "http://metadata-service:8001"


class RetrievalConfig(BaseModel):
    metadata_service: ServiceConfig
    kv_service: ServiceConfig
    historian_service: Optional[ServiceConfig] = None
    cfs_service: Optional[ServiceConfig] = None


def load_config() -> RetrievalConfig:
    """
    Load configuration from the file pointed to by MFI_RETRIEVAL_CONFIG.

    The format is a YAML/JSON object compatible with `RetrievalConfig`.
    """
    path = os.getenv("MFI_RETRIEVAL_CONFIG")
    if not path:
        raise RuntimeError("MFI_RETRIEVAL_CONFIG environment variable is not set")
    with open(path, "r") as f:
        raw = yaml.safe_load(f)
    return RetrievalConfig.model_validate(raw)


def get_config() -> RetrievalConfig:
    # Loaded once per process; FastAPI dependency handles reuse.
    # If hot‑reloading config is needed, this can be extended.
    return load_config()


###############################################################################
# FastAPI app
###############################################################################

app = FastAPI(title="MFI Retrieval API", version="0.1.0")


###############################################################################
# Metadata store endpoints (proxy to metadata service)
###############################################################################


class TrialFilter(BaseModel):
    project_id: Optional[str] = None
    user_id: Optional[str] = None
    user_domain: Optional[str] = None
    trial_name: Optional[str] = None


@app.get("/metadata/trials", summary="List trials from metadata store")
def list_trials(
    project_id: Optional[str] = Query(default=None),
    user_id: Optional[str] = Query(default=None),
    user_domain: Optional[str] = Query(default=None),
    trial_name: Optional[str] = Query(default=None),
    cfg: RetrievalConfig = Depends(get_config),
) -> List[Dict[str, Any]]:
    """
    Simple read‑only filter over `trial_detail` table.

    NOTE: write‑side helpers (insert/update) already live in
    `metadata_utils.py`. This endpoint is purely for retrieval.
    """
    params: Dict[str, Any] = {}
    if project_id is not None:
        params["project_id"] = project_id
    if user_id is not None:
        params["user_id"] = user_id
    if user_domain is not None:
        params["user_domain"] = user_domain
    if trial_name is not None:
        params["trial_name"] = trial_name

    url = f"{cfg.metadata_service.base_url.rstrip('/')}/trials"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, params=params)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Metadata service error: {resp.text}",
            )
        data = resp.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500,
                detail="Metadata service returned unexpected payload",
            )
        return data
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to reach metadata service: {exc}",
        ) from exc


###############################################################################
# KV store endpoints (proxy to KV service)
###############################################################################


class KVItem(BaseModel):
    id: str
    trial_id: str
    payload: Dict[str, Any]
    created_at: datetime


@app.get("/kv/items", response_model=List[KVItem], summary="Retrieve KV items")
def get_kv_items(
    trial_id: Optional[str] = Query(default=None),
    cfg: RetrievalConfig = Depends(get_config),
) -> List[KVItem]:
    """
    Retrieve key–value payloads from the KV store table.

    Expected table structure (see docs):
        CREATE TABLE kv_store (
            id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            trial_id   TEXT NOT NULL,
            payload    JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
    """
    import psycopg2
    import psycopg2.extras

    params: Dict[str, Any] = {}
    if trial_id is not None:
        params["trial_id"] = trial_id

    url = f"{cfg.kv_service.base_url.rstrip('/')}/items"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url, params=params)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"KV service error: {resp.text}",
            )
        data = resp.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500, detail="KV service returned unexpected payload"
            )
        return [KVItem.model_validate(item) for item in data]
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to reach KV service: {exc}",
        ) from exc


###############################################################################
# Historian retrieval endpoints (proxy to historian service)
###############################################################################


class HistorianPoint(BaseModel):
    timestamp: datetime
    value: float


@app.get(
    "/historian/query",
    response_model=List[HistorianPoint],
    summary="Query historian time‑series data",
)
def query_historian(
    tag: str = Query(..., description="Historian tag / PI point name"),
    start: datetime = Query(..., description="Start time (ISO‑8601)"),
    end: datetime = Query(..., description="End time (ISO‑8601)"),
    cfg: RetrievalConfig = Depends(get_config),
) -> List[HistorianPoint]:
    """
    Historian retrieval is implemented as a very thin wrapper around a
    project‑specific connector (e.g. Aveva PI Web API client).

    To hook this up in your environment, implement a small function
    `query_historian_points(dsn, tag, start, end)` that returns
    `(timestamp, value)` rows and call it from here.
    """
    if not cfg.historian_service:
        raise HTTPException(
            status_code=503, detail="Historian service is not configured"
        )

    params = {
        "tag": tag,
        "start": start.isoformat(),
        "end": end.isoformat(),
    }
    url = f"{cfg.historian_service.base_url.rstrip('/')}/query"
    try:
        with httpx.Client(timeout=20.0) as client:
            resp = client.get(url, params=params)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"Historian service error: {resp.text}",
            )
        data = resp.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500,
                detail="Historian service returned unexpected payload",
            )
        return [HistorianPoint.model_validate(item) for item in data]
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to reach historian service: {exc}",
        ) from exc


###############################################################################
# CFS retrieval endpoints (proxy to CFS service)
###############################################################################


class CFSObject(BaseModel):
    id: str
    file_path: str
    metadata_path: str
    size: int
    timestamp: Optional[int] = None
    trial_id: Optional[str] = None


@app.get("/cfs/objects", response_model=List[CFSObject], summary="List CFS objects")
def list_cfs_objects(
    cfg: RetrievalConfig = Depends(get_config),
) -> List[CFSObject]:
    if not cfg.cfs:
        raise HTTPException(
            status_code=503, detail="CFS connector is not configured"
        )
    if not cfg.cfs_service:
        raise HTTPException(
            status_code=503, detail="CFS service is not configured"
        )

    url = f"{cfg.cfs_service.base_url.rstrip('/')}/objects"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"CFS service error: {resp.text}",
            )
        data = resp.json()
        if not isinstance(data, list):
            raise HTTPException(
                status_code=500, detail="CFS service returned unexpected payload"
            )
        return [CFSObject.model_validate(item) for item in data]
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to reach CFS service: {exc}",
        ) from exc


@app.get(
    "/cfs/objects/{object_id}", response_model=CFSObject, summary="Get a single CFS object"
)
def get_cfs_object(
    object_id: str,
    cfg: RetrievalConfig = Depends(get_config),
) -> CFSObject:
    if not cfg.cfs_service:
        raise HTTPException(
            status_code=503, detail="CFS service is not configured"
        )

    url = f"{cfg.cfs_service.base_url.rstrip('/')}/objects/{object_id}"
    try:
        with httpx.Client(timeout=10.0) as client:
            resp = client.get(url)
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Object not found")
        if resp.status_code != 200:
            raise HTTPException(
                status_code=resp.status_code,
                detail=f"CFS service error: {resp.text}",
            )
        data = resp.json()
        return CFSObject.model_validate(data)
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=503,
            detail=f"Failed to reach CFS service: {exc}",
        ) from exc


###############################################################################
# Unified retrieval entry point
###############################################################################


class TrialDataResponse(BaseModel):
    trial_name: str
    metadata: Optional[Dict[str, Any]] = None
    kv_items: Optional[List[KVItem]] = None
    historian: Optional[List[HistorianPoint]] = None
    cfs_objects: Optional[List[CFSObject]] = None


@app.get(
    "/retrieve/trials/{trial_name}",
    response_model=TrialDataResponse,
    summary="Main retrieval endpoint that fans out to underlying stores",
)
def retrieve_trial_data(
    trial_name: str,
    include_metadata: bool = Query(
        default=True, description="Include trial metadata from metadata store"
    ),
    include_kv: Optional[bool] = Query(
        default=None,
        description="Include KV items; defaults to True for this trial if not set",
    ),
    include_historian: Optional[bool] = Query(
        default=None,
        description=(
            "Include historian points; defaults to True when metadata "
            "indicates a historian topic family"
        ),
    ),
    include_cfs: Optional[bool] = Query(
        default=None,
        description="Include CFS objects for this trial; defaults to True if not set",
    ),
    # Optional time window for historian queries; if omitted we default to
    # birth/death from the metadata row when available.
    start: Optional[datetime] = Query(
        default=None, description="Override start time for historian queries"
    ),
    end: Optional[datetime] = Query(
        default=None, description="Override end time for historian queries"
    ),
    cfg: RetrievalConfig = Depends(get_config),
) -> TrialDataResponse:
    """
    Unified entry point: look up the trial in the metadata store and,
    based on that metadata, decide which backing databases to query.

    This keeps the routing logic centralized while individual stores
    remain independent and testable.
    """
    # ------------------------------------------------------------------
    # 1) Fetch trial row + full metadata via metadata service
    # ------------------------------------------------------------------
    trial_list = list_trials(
        project_id=None,
        user_id=None,
        user_domain=None,
        trial_name=trial_name,
        cfg=cfg,
    )
    if not trial_list:
        raise HTTPException(
            status_code=404, detail=f"No trial_detail row found for '{trial_name}'"
        )
    # Use the most recent row
    trial_row = trial_list[0]
    trial_metadata = trial_row.get("metadata") or {}

    # Determine default routing based on metadata if caller didn't specify.
    broker_block = (trial_metadata.get("broker") or {})
    topic_family = broker_block.get("topic_family")

    if include_historian is None:
        include_historian = topic_family == "historian"
    if include_kv is None:
        include_kv = True
    if include_cfs is None:
        include_cfs = True

    # ------------------------------------------------------------------
    # 2) Collect data from each underlying store as requested
    # ------------------------------------------------------------------
    result = TrialDataResponse(trial_name=trial_name)

    # a) Metadata
    if include_metadata:
        result.metadata = trial_metadata

    # b) KV store
    if include_kv:
        # Reuse the KV REST endpoint locally
        kv_rows = get_kv_items(trial_id=trial_name, cfg=cfg)
        result.kv_items = kv_rows

    # c) Historian (time-series)
    if include_historian:
        # Use birth/death timestamps from DB when explicit range is not supplied.
        start_ts = start or trial_row.get("birth_timestamp")
        end_ts = end or trial_row.get("death_timestamp") or datetime.utcnow()

        if not isinstance(start_ts, datetime) or not isinstance(end_ts, datetime):
            raise HTTPException(
                status_code=500,
                detail="Metadata trial row did not contain valid timestamps",
            )

        result.historian = query_historian(
            tag=trial_name, start=start_ts, end=end_ts, cfg=cfg
        )

    # d) CFS (cloud file storage)
    if include_cfs:
        if not cfg.cfs_service:
            raise HTTPException(
                status_code=503, detail="CFS service is not configured"
            )

        # Fetch all objects and filter by trial_id client-side.
        all_objs = list_cfs_objects(cfg=cfg)
        trial_objs = [
            o for o in all_objs if (o.trial_id is None or o.trial_id == trial_name)
        ]
        result.cfs_objects = trial_objs

    return result


###############################################################################
# Health check
###############################################################################


@app.get("/health", summary="Health check")
def health() -> Dict[str, str]:
    return {"status": "ok"}


