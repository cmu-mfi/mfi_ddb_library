"""
Search previous entries of a form through the retrieval API (RWS type1).

Every submission carries a hidden `form_id` and becomes a trial in the
metadata store; the submitted payload is kept in the trial metadata
(adapter.config.payloads). So:

  1. RWS type1 with search_terms=[form_id] (+ free text) and the time window
     finds the candidate trials - access control is RWS's: a requester sees
     their own trials (and projects they administer).
  2. The payloads of this form are pulled out of each trial.
  3. Field filters (contains / one of / min-max / from-to) are applied here,
     because RWS has no structured metadata filters yet.

`entries.show_all` (default true for now) queries RWS as the superadmin so
everyone sees every entry; set it to false to only show the viewer's own.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field

from app.config import FormConfig, FormDef

logger = logging.getLogger(__name__)

SUPERADMIN = ("superadmin", "superadmin")
EARLIEST = datetime(2000, 1, 1, tzinfo=timezone.utc)
FILTERABLE_TYPES = {"text", "textarea", "email", "select", "number", "integer", "date", "datetime", "checkbox"}


class EntriesError(Exception):
    def __init__(self, message: str, status: int = 502):
        super().__init__(message)
        self.status = status


class Viewer(BaseModel):
    user_id: str
    domain: str


class FieldFilter(BaseModel):
    model_config = ConfigDict(extra="forbid")
    contains: Optional[str] = None          # text
    any_of: Optional[List[str]] = None      # select
    min: Optional[float] = None             # number / integer
    max: Optional[float] = None
    date_from: Optional[str] = None         # date / datetime (ISO)
    date_to: Optional[str] = None
    equals: Optional[bool] = None           # checkbox


class EntriesQuery(BaseModel):
    model_config = ConfigDict(extra="forbid")
    viewer: Optional[Viewer] = None
    entered_from: Optional[str] = Field(None, description="ISO 8601; default: no lower bound")
    entered_to: Optional[str] = Field(None, description="ISO 8601; default: now")
    text: Optional[str] = Field(None, description="Free-text search (RWS search_terms)")
    filters: Dict[str, FieldFilter] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _parse_dt(value: Any) -> Optional[datetime]:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def _call_type1(cfg: FormConfig, body: dict) -> List[dict]:
    url = cfg.entries.rws_url.rstrip("/") + "/type1"
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=cfg.entries.timeout_seconds) as resp:
            data = json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as err:
        detail = err.read().decode(errors="replace")[:300]
        raise EntriesError(f"Retrieval API returned {err.code}: {detail}") from None
    except (urllib.error.URLError, TimeoutError, OSError) as err:
        reason = getattr(err, "reason", err)
        raise EntriesError(f"Cannot reach the retrieval API at {url}: {reason}", 503) from None
    if data.get("status") != "success":
        raise EntriesError(f"Retrieval API error: {data.get('message') or data}")
    return data.get("trials") or []


def _payloads_of(trial: dict, form_id: str) -> List[dict]:
    adapter = (trial.get("metadata") or {}).get("adapter") or {}
    payloads = (adapter.get("config") or {}).get("payloads") or []
    return [p for p in payloads if isinstance(p, dict) and p.get("form_id") == form_id]


def _matches(value: Any, field_type: str, flt: FieldFilter) -> bool:
    if flt.contains:
        if value is None or flt.contains.strip().lower() not in str(value).lower():
            return False
    if flt.any_of:
        if value is None or str(value) not in {str(v) for v in flt.any_of}:
            return False
    if flt.min is not None or flt.max is not None:
        try:
            number = float(value)
        except (TypeError, ValueError):
            return False
        if flt.min is not None and number < flt.min:
            return False
        if flt.max is not None and number > flt.max:
            return False
    if flt.date_from or flt.date_to:
        if value in (None, ""):
            return False
        if field_type == "date":  # plain YYYY-MM-DD compares as text
            day = str(value)[:10]
            if flt.date_from and day < flt.date_from[:10]:
                return False
            if flt.date_to and day > flt.date_to[:10]:
                return False
        else:
            when = _parse_dt(value)
            lo, hi = _parse_dt(flt.date_from), _parse_dt(flt.date_to)
            if when is None or (lo and when < lo) or (hi and when > hi):
                return False
    if flt.equals is not None and bool(value) != flt.equals:
        return False
    return True


# ---------------------------------------------------------------------------
# search
# ---------------------------------------------------------------------------


def search_entries(cfg: FormConfig, form: FormDef, query: EntriesQuery) -> dict:
    fields = {f.name: f for f in form.fields}
    for name in query.filters:
        field = fields.get(name)
        if field is None or field.type not in FILTERABLE_TYPES or field.hidden:
            raise EntriesError(f"'{name}' is not a filterable field of form '{form.id}'", 422)

    if cfg.entries.show_all:
        requester = SUPERADMIN
    elif query.viewer is None:
        raise EntriesError("Pick who you are to see your entries.", 422)
    else:
        requester = (query.viewer.user_id, query.viewer.domain)

    now = datetime.now(timezone.utc)
    start = _parse_dt(query.entered_from) or EARLIEST
    end = _parse_dt(query.entered_to) or now + timedelta(minutes=5)
    if start > end:
        raise EntriesError("'Entered from' is after 'Entered to'.", 422)

    terms = [form.id] + [t for t in (query.text or "").split() if t]
    trials = _call_type1(cfg, {
        "time_start": _iso(start),
        "time_end": _iso(end),
        "user_id": requester[0],
        "user_domain": requester[1],
        "search_terms": terms,
    })

    # Every version of every entry, grouped by entry_id. Entries saved before
    # versioning existed have no entry_id: they are version 1 of an entry whose
    # id is their trial uuid (so they can be updated too).
    groups: Dict[str, List[dict]] = {}
    for trial in trials:
        for payload in _payloads_of(trial, form.id):
            entered = _parse_dt(payload.get("timestamp")) or _parse_dt(trial.get("birth_timestamp"))
            try:
                version = int(payload.get("entry_version") or 1)
            except (TypeError, ValueError):
                version = 1
            entry_id = str(payload.get("entry_id") or trial.get("uuid"))
            groups.setdefault(entry_id, []).append({
                "uuid": trial.get("uuid"),
                "entry_id": entry_id,
                "version": version,
                "trial_id": payload.get("trial_id") or trial.get("trial_name"),
                "entered_at": _iso(entered) if entered else None,
                "created_by": {
                    "user_id": payload.get("created_by_user_id"),
                    "domain": payload.get("created_by_domain"),
                },
                "values": payload,
                "_entered": entered,
            })

    entries = []
    for versions in groups.values():
        # newest version first; same version number (two parallel updates) -> newest entry
        versions.sort(key=lambda v: (v["version"], v["entered_at"] or ""), reverse=True)
        latest = versions[0]
        # filters and the time window apply to the current (latest) version
        entered = latest.pop("_entered")
        if entered and not (start <= entered <= end):
            continue
        if not all(_matches(latest["values"].get(n), fields[n].type, f) for n, f in query.filters.items()):
            continue
        for older in versions[1:]:
            older.pop("_entered", None)
        entries.append({**latest, "versions": versions[1:]})

    entries.sort(key=lambda e: e["entered_at"] or "", reverse=True)
    return {
        "ok": True,
        "form_id": form.id,
        "show_all": cfg.entries.show_all,
        "count": len(entries),
        "entries": entries[: cfg.entries.max_results],
        "truncated": len(entries) > cfg.entries.max_results,
    }
