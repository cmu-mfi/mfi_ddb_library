"""
User directory: look up users registered in the metadata store.

Calls the Retrieval API (RWS) endpoints
    GET /mfi-ddb/users?search=&limit=
    GET /mfi-ddb/users/{domain}/{user_id}
identifying itself with X-User-Id / X-User-Domain (directory.requester_* in
the config). With single sign-on those headers come from the login proxy
instead; nothing else here changes.

Lookups never block a submission: if RWS is unreachable the form falls back
to free text and submissions carry a warning instead.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from app.config import FieldDef, FormConfig, FormDef

logger = logging.getLogger(__name__)


class DirectoryUnavailable(Exception):
    pass


def _get(cfg: FormConfig, path: str, params: Optional[dict] = None) -> Tuple[int, Any]:
    d = cfg.directory
    url = f"{d.rws_url.rstrip('/')}/mfi-ddb{path}"
    if params:
        url += "?" + urllib.parse.urlencode({k: v for k, v in params.items() if v not in (None, "")})
    req = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "X-User-Id": d.requester_user_id,
            "X-User-Domain": d.requester_domain,
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=d.timeout_seconds) as res:
            return res.status, json.loads(res.read() or b"null")
    except urllib.error.HTTPError as err:
        try:
            body = json.loads(err.read() or b"null")
        except ValueError:
            body = None
        if err.code == 404:
            return 404, body
        detail = (body or {}).get("detail") if isinstance(body, dict) else None
        raise DirectoryUnavailable(f"Retrieval API answered {err.code}: {detail or err.reason}") from None
    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as err:
        raise DirectoryUnavailable(f"Retrieval API not reachable at {d.rws_url}: {err}") from None


def search_users(cfg: FormConfig, search: Optional[str], limit: int = 25) -> List[Dict[str, Any]]:
    _, body = _get(cfg, "/users", {"search": search, "limit": limit})
    return (body or {}).get("users", [])


def get_user(cfg: FormConfig, user_id: str, domain: str) -> Optional[Dict[str, Any]]:
    path = f"/users/{urllib.parse.quote(domain or '-', safe='')}/{urllib.parse.quote(user_id, safe='')}"
    status, body = _get(cfg, path)
    if status == 404:
        return None
    return (body or {}).get("user")


# ---------------------------------------------------------------------------
# Which users does a payload reference?
# ---------------------------------------------------------------------------


def _refs(fields: List[FieldDef], values: dict, prefix: str) -> List[Tuple[str, str, str]]:
    refs = []
    for f in fields:
        if f.type == "list" and f.fields:
            for i, item in enumerate(values.get(f.name) or []):
                if isinstance(item, dict):
                    refs.extend(_refs(f.fields, item, f"{prefix}{f.name}.{i}."))
        elif f.lookup == "user":
            user_id = values.get(f.name)
            if isinstance(user_id, str) and user_id.strip():
                domain = values.get(f.domain_field, "") if f.domain_field else ""
                refs.append((f"{prefix}{f.name}", user_id.strip(), str(domain or "").strip()))
    return refs


def user_refs(form: FormDef, payload: dict) -> List[Tuple[str, str, str]]:
    """[(field path, user_id, domain)] for every user-lookup field that has a value."""
    return _refs(form.fields, payload, "")


def reserved_refs(form: FormDef, payload: dict, cfg: FormConfig) -> List[dict]:
    """Errors for reserved platform accounts (e.g. superadmin) used in a user field.

    Enforced whether or not the directory is reachable; the caller lets
    platform admins (admin token) through.
    """
    return [
        {
            "field": path,
            "message": f"{user_id} / {domain or '-'} is reserved for platform admins. "
            "Enter the admin token (Settings tab) to use it.",
        }
        for path, user_id, domain in user_refs(form, payload)
        if cfg.directory.is_reserved(user_id, domain)
    ]


def check_users(form: FormDef, payload: dict, cfg: FormConfig) -> List[dict]:
    """Errors for referenced users that are not registered (a form is refused, like a
    missing required field). If the directory can't be reached, the form is refused
    too, since registration can't be confirmed. Directory off = no check."""
    if not cfg.directory.active:
        return []
    refs = [r for r in user_refs(form, payload) if not cfg.directory.is_reserved(r[1], r[2])]
    if not refs:
        return []
    errors = []
    seen: Dict[Tuple[str, str], Optional[dict]] = {}
    try:
        for path, user_id, domain in refs:
            key = (user_id, domain)
            if key not in seen:
                seen[key] = get_user(cfg, user_id, domain)
            if seen[key] is None:
                errors.append(
                    {
                        "field": path,
                        "message": f"{user_id} / {domain or '-'} is not a registered user. "
                        "Register them first with the New user form.",
                    }
                )
    except DirectoryUnavailable as err:
        logger.warning("User check failed: %s", err)
        return [
            {
                "field": None,
                "message": "Could not confirm that the people entered are registered "
                f"(user directory unavailable: {err}). Try again shortly.",
            }
        ]
    return errors
