"""
Cloud File Storage (CFS) retrieval helpers.

These functions understand the on-disk layout produced by
`databases/cfs/store_cfs.py` and expose a simple, typed view over the
stored files and their JSON sidecar metadata.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import List, Optional


@dataclass
class CFSObject:
    """
    Lightweight representation of a stored CFS object.
    """

    id: str
    file_path: str
    metadata_path: str
    size: int
    timestamp: Optional[int] = None
    trial_id: Optional[str] = None


def list_objects(base_directory: str) -> List[CFSObject]:
    """
    List all CFS objects found under `base_directory`.

    Conventions (as used by `store_cfs.py`):
      - binary files: <uid>.<ext>
      - metadata JSON: <uid>.json
    """
    if not os.path.isdir(base_directory):
        return []

    by_id = {}

    for name in os.listdir(base_directory):
        path = os.path.join(base_directory, name)
        if not os.path.isfile(path):
            continue

        stem, ext = os.path.splitext(name)
        rec = by_id.setdefault(stem, {"id": stem})
        if ext == ".json":
            rec["metadata_path"] = path
        else:
            rec["file_path"] = path
            rec["size"] = os.path.getsize(path)

    objects: List[CFSObject] = []
    for rec in by_id.values():
        ts: Optional[int] = None
        meta_path = rec.get("metadata_path")
        trial_id: Optional[str] = None
        if meta_path and os.path.isfile(meta_path):
            with open(meta_path, "r") as f:
                meta = json.load(f)
            if "timestamp" in meta:
                try:
                    ts = int(meta["timestamp"])
                except (ValueError, TypeError):
                    ts = None
            trial_id = meta.get("trial_id")

        objects.append(
            CFSObject(
                id=rec["id"],
                file_path=rec.get("file_path", ""),
                metadata_path=rec.get("metadata_path", ""),
                size=int(rec.get("size", 0)),
                timestamp=ts,
                trial_id=trial_id,
            )
        )

    return objects


def get_object(base_directory: str, object_id: str) -> Optional[CFSObject]:
    """
    Fetch a single CFS object by ID, or return None if not found.
    """
    objs = {o.id: o for o in list_objects(base_directory)}
    return objs.get(object_id)


