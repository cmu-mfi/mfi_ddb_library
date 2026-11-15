"""
Split an entry by weight (Entries page -> Split).

The person fills in a form pre-filled from the original entry and enters the
weight W to take. Saving publishes two payloads:

  1. a new entry (weight W)                       + split_from = original entry_id
  2. the original's next version (weight P - W)   + split_into = new entry_id

Both are built and validated before either is published. The subtraction is
done here, never in the browser. Allowed: 0 < W <= P.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, ConfigDict, Field

from app import entries
from app.config import FormConfig, FormDef
from app.payload import Revision, SubmissionError, build_payload


class SplitError(Exception):
    def __init__(self, message: str, status: int = 422):
        super().__init__(message)
        self.status = status


class Split(BaseModel):
    """The entry being split: the version it was opened at and its form values."""

    model_config = ConfigDict(extra="forbid")
    revises: Revision
    values: Dict[str, Any] = Field(default_factory=dict)


class PreparedSplit(BaseModel):
    child: dict
    parent: dict
    remaining: float
    key: str
    # what a retry of step 2 alone needs (normal /submit with `revises`)
    parent_values: Dict[str, Any]
    revises: Revision


def split_key(cfg: FormConfig, form: FormDef) -> Optional[str]:
    if not cfg.splits.enabled:
        return None
    key = cfg.splits.forms.get(form.id)
    field = next((f for f in form.fields if f.name == key), None) if key else None
    return key if field is not None and field.type in ("number", "integer") else None


def _number(value: Any) -> Optional[float]:
    if value is None or isinstance(value, bool) or (isinstance(value, str) and not value.strip()):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _with_link(payload: dict, name: str, value: str) -> dict:
    """Insert a hidden link key right after the version keys (before the form's values)."""
    out: dict = {}
    for k, v in payload.items():
        out[k] = v
        if k in ("supersedes",) or (k == "entry_version" and "supersedes" not in payload):
            out[name] = value
    out.setdefault(name, value)
    return out


def prepare_split(cfg: FormConfig, form: FormDef, values: dict, split: Split) -> PreparedSplit:
    key = split_key(cfg, form)
    if key is None:
        raise SplitError(f"Form '{form.id}' can't be split.")
    field = next(f for f in form.fields if f.name == key)

    # the new entry (all its field errors come back the usual way)
    child = build_payload(form, values, cfg)
    taken = _number(child.get(key))
    if taken is None or taken <= 0:
        raise SubmissionError([{
            "field": key, "message": f"Enter the {field.display_label.lower()} to split off (more than 0).",
        }])

    total = _number(split.values.get(key))
    if total is None:
        raise SplitError(f"The original entry has no {field.display_label.lower()} to split.")
    if taken > total + 1e-9:
        raise SubmissionError([{
            "field": key, "message": f"Can't split off more than the original's {total:g}.",
        }])

    remaining = max(round(total - taken, 6), 0.0)
    parent_values = dict(split.values)
    parent_values[key] = int(remaining) if field.type == "integer" else remaining
    # the person splitting is recorded on the original's new version too
    if "created_by" in values:
        parent_values["created_by"] = values["created_by"]
    try:
        parent = build_payload(form, parent_values, cfg, revision=split.revises)
    except SubmissionError as err:
        names = ", ".join(sorted({e.get("field") or "?" for e in err.errors}))
        raise SplitError(
            f"The original entry can't be saved as a new version ({names}): "
            + "; ".join(e["message"] for e in err.errors)
        ) from None

    child = _with_link(child, "split_from", split.revises.entry_id)
    parent = _with_link(parent, "split_into", child["entry_id"])
    return PreparedSplit(
        child=child, parent=parent, remaining=remaining, key=key,
        parent_values=parent_values, revises=split.revises,
    )


def check_not_stale(cfg: FormConfig, form: FormDef, revises: Revision) -> None:
    """Refuse when the original has a newer version than the one the split was opened at."""
    if not cfg.entries.enabled:
        return
    end = datetime.now(timezone.utc) + timedelta(minutes=5)

    def versions(terms) -> list:
        trials = entries._call_type1(cfg, {
            "time_start": entries._iso(entries.EARLIEST),
            "time_end": entries._iso(end),
            "user_id": entries.SUPERADMIN[0],
            "user_domain": entries.SUPERADMIN[1],
            "search_terms": terms,
        })
        found = []
        for trial in trials:
            for payload in entries._payloads_of(trial, form.id):
                if str(payload.get("entry_id") or trial.get("uuid")) != revises.entry_id:
                    continue
                try:
                    found.append(int(payload.get("entry_version") or 1))
                except (TypeError, ValueError):
                    found.append(1)
        return found

    try:
        found = versions([form.id, revises.entry_id])
        if not found and revises.entry_id == revises.trial_uuid:
            found = versions([form.id])  # saved before versioning: no entry_id in the payload
    except entries.EntriesError as err:
        raise SplitError(f"Can't check the original entry before splitting: {err}", 503) from None

    if not found:
        raise SplitError(
            "The original entry can't be found in the retrieval API yet. Try again in a moment.", 409
        )
    latest = max(found)
    if latest > revises.version:
        raise SplitError(
            f"This entry changed since you opened it (now version {latest}). "
            "Close the form and split it again from Entries.",
            409,
        )
