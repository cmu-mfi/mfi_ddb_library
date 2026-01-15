"""
Cloud File Storage (CFS) service.

Thin REST wrapper around the on-disk layout written by
`databases/cfs/store_cfs.py`. The Retrieval API talks to this service
over HTTP.

Run:

    uvicorn mfi_ddb.databases.cfs_service:app --reload --port 8003

Environment:
    CFS_BASE_DIRECTORY  (directory where files + metadata JSON are stored)
"""

from __future__ import annotations

import os
from typing import Dict, List

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from mfi_ddb.databases.cfs.retrieval import (
    CFSObject as RawCFSObject,
    get_object,
    list_objects,
)


class CFSObject(BaseModel):
    id: str
    file_path: str
    metadata_path: str
    size: int
    timestamp: int | None = None
    trial_id: str | None = None


app = FastAPI(title="CFS Service", version="0.1.0")


def _base_dir() -> str:
    base = os.environ.get("CFS_BASE_DIRECTORY")
    if not base:
        raise RuntimeError("CFS_BASE_DIRECTORY environment variable is not set")
    return base


@app.get("/objects", response_model=List[CFSObject], summary="List CFS objects")
def list_cfs_objects() -> List[CFSObject]:
    try:
        objs = list_objects(_base_dir())
        return [
            CFSObject(
                id=o.id,
                file_path=o.file_path,
                metadata_path=o.metadata_path,
                size=o.size,
                timestamp=o.timestamp,
                trial_id=o.trial_id,
            )
            for o in objs
        ]
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get(
    "/objects/{object_id}",
    response_model=CFSObject,
    summary="Get a single CFS object",
)
def get_cfs_object(object_id: str) -> CFSObject:
    try:
        obj: RawCFSObject | None = get_object(_base_dir(), object_id)
        if obj is None:
            raise HTTPException(status_code=404, detail="Object not found")
        return CFSObject(
            id=obj.id,
            file_path=obj.file_path,
            metadata_path=obj.metadata_path,
            size=obj.size,
            timestamp=obj.timestamp,
            trial_id=obj.trial_id,
        )
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


@app.get("/health", summary="Health check")
def health() -> Dict[str, str]:
    try:
        list_objects(_base_dir())
    except Exception as exc:  # pragma: no cover - simple health check
        raise HTTPException(status_code=503, detail=str(exc))
    return {"status": "ok"}


