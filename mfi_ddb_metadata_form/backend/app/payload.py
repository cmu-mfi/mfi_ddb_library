"""
Turn submitted form values into an MFI DDB key-value payload.

Steps:
  1. For every configured field: take the user value (or the auto / hidden
     value), coerce it to the field type, and enforce required / options /
     pattern / min / max.
  2. Wrap the result with schema_version + msg_type.
  3. Validate the final payload against the library's kv.json schema - the
     same check KvDataAdapter and the KV topic family apply - so anything
     accepted here will also be accepted downstream.

Errors are returned as a list of {"field": "<dotted.path>" | None, "message": str}
so the UI can show them next to the right input.
"""

from __future__ import annotations

import copy
import functools
import json
import re
import uuid
from datetime import date, datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from app.config import FieldDef, FormConfig, FormDef, resolve_auto
from app.users import UserDirectoryError, UserKey, directory

# Returns {(user_id, domain): user} for the registered users; may raise UserDirectoryError.
UserLookup = Callable[[], Dict[UserKey, dict]]

SCHEMA_VERSION = "mfi-v1.0"

_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
_REQUIRED_MSG = re.compile(r"^'(.+)' is a required property$")
_MISSING = object()


class SubmissionError(Exception):
    def __init__(self, errors: List[dict]):
        super().__init__("; ".join(e["message"] for e in errors))
        self.errors = errors


# ---------------------------------------------------------------------------
# kv.json schema helpers
# ---------------------------------------------------------------------------


@functools.lru_cache(maxsize=1)
def _validator():
    from mfi_ddb.topic_families.key_value import KeyValueTopicFamily

    return KeyValueTopicFamily.get_schema_validator()


def _clause_msg_types(condition: dict) -> List[str]:
    """msg_types an `if` clause of kv.json applies to."""
    const = condition.get("properties", {}).get("msg_type", {}).get("const")
    if const:
        return [const]
    types = []
    for sub in condition.get("anyOf", []):
        types.extend(_clause_msg_types(sub))
    return types


def kv_requirements(msg_type: str) -> Tuple[set, List[List[str]]]:
    """(required keys, 'at least one of' groups) kv.json imposes on a msg_type."""
    schema = _validator().schema
    required = set(schema.get("required", [])) - {"schema_version", "msg_type"}
    any_of: List[List[str]] = []
    for clause in schema.get("allOf", []):
        if msg_type not in _clause_msg_types(clause.get("if", {})):
            continue
        then = clause.get("then", {})
        required |= set(then.get("required", []))
        group = [r for sub in then.get("anyOf", []) for r in sub.get("required", [])]
        if group:
            any_of.append(group)
    return required, any_of


def lint_config(cfg: FormConfig) -> List[str]:
    """Warn about forms that can never produce a schema-valid payload."""
    warnings = []
    for form in cfg.forms:
        if not form.enabled:
            continue
        provided = {
            k for f in form.fields if not f.hidden or f.default is not None for k in f.payload_keys
        }
        guaranteed = {
            k
            for f in form.fields
            if f.required or (f.auto and not f.editable) or (f.hidden and f.default is not None)
            for k in f.payload_keys
        }
        if not form.has_required_user:
            warnings.append(
                f"form '{form.id}': add a required field of type 'user' (e.g. created_by) so "
                "every submission names a registered user"
            )
        required, any_of = kv_requirements(form.msg_type)
        for key in sorted(required - provided):
            warnings.append(
                f"form '{form.id}': kv schema requires '{key}' for msg_type "
                f"'{form.msg_type}' but the form has no such field"
            )
        for key in sorted((required & provided) - guaranteed):
            warnings.append(
                f"form '{form.id}': '{key}' is required by the kv schema; "
                "mark the field required (or auto) so users see it up front"
            )
        for group in any_of:
            if not provided.intersection(group):
                warnings.append(
                    f"form '{form.id}': kv schema needs at least one of {group} "
                    f"for msg_type '{form.msg_type}'"
                )
        if form.msg_type == "data" and "trial_id" not in provided:
            warnings.append(
                f"form '{form.id}': msg_type 'data' needs a trial_id field; the KV adapter "
                "drops 'data' payloads without one"
            )
    return warnings


def schema_errors(payload: dict) -> List[dict]:
    candidate = copy.deepcopy(payload)
    from mfi_ddb.topic_families.key_value import KeyValueTopicFamily

    KeyValueTopicFamily.apply_defaults(candidate, _validator())
    errors = []
    for err in sorted(_validator().iter_errors(candidate), key=lambda e: list(e.absolute_path)):
        path = ".".join(str(p) for p in err.absolute_path)
        if err.validator == "required":
            match = _REQUIRED_MSG.match(err.message)
            missing = match.group(1) if match else None
            field = ".".join(p for p in (path, missing) if p) or None
            errors.append({"field": field, "message": f"'{missing}' is required by the kv schema"})
        elif err.validator == "anyOf" and all(
            set(s.keys()) == {"required"} for s in err.validator_value
        ):
            keys = [k for s in err.validator_value for k in s["required"]]
            errors.append(
                {"field": path or None, "message": f"Provide at least one of: {', '.join(keys)}"}
            )
        else:
            errors.append({"field": path or None, "message": err.message})
    return errors


# ---------------------------------------------------------------------------
# Value coercion
# ---------------------------------------------------------------------------


def _is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    if isinstance(value, (list, dict)) and len(value) == 0:
        return True
    return False


def _parse_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    parsed = parsed.astimezone(timezone.utc)
    return parsed.isoformat(timespec="seconds").replace("+00:00", "Z")


def _split_user(raw: Any) -> Optional[UserKey]:
    """{"user_id", "domain"} (or "user_id@domain") -> (user_id, domain); None if blank."""
    if isinstance(raw, dict):
        user_id, domain = raw.get("user_id"), raw.get("domain")
    elif isinstance(raw, str) and "@" in raw:
        user_id, domain = raw.rsplit("@", 1)
    else:
        user_id, domain = raw, None
    user_id = "" if user_id is None else str(user_id).strip()
    domain = "" if domain is None else str(domain).strip()
    if not user_id and not domain:
        return None
    return user_id, domain


def _coerce(
    field: FieldDef, raw: Any, cfg: FormConfig, path: str, errors: List[dict], users: "UserLookup"
) -> Any:
    label = field.display_label

    def fail(message: str):
        errors.append({"field": path, "message": message})
        return _MISSING

    t = field.type
    try:
        if t in ("text", "textarea", "email"):
            if isinstance(raw, (dict, list)):
                return fail(f"{label} must be text")
            value = str(raw).strip()
            if t == "email" and not _EMAIL_PATTERN.match(value):
                return fail(f"{label} must be a valid email address")
            if field.pattern and not re.fullmatch(field.pattern, value):
                return fail(f"{label} does not match the required format")
            return value

        if t in ("number", "integer"):
            if isinstance(raw, bool):
                return fail(f"{label} must be a number")
            if t == "integer":
                number = int(raw) if not isinstance(raw, float) or raw.is_integer() else None
                if number is None:
                    return fail(f"{label} must be a whole number")
            else:
                number = float(raw)
            if field.min is not None and number < field.min:
                return fail(f"{label} must be at least {field.min:g}")
            if field.max is not None and number > field.max:
                return fail(f"{label} must be at most {field.max:g}")
            return number

        if t == "checkbox":
            if isinstance(raw, bool):
                return raw
            text = str(raw).strip().lower()
            if text in ("true", "1", "yes", "on"):
                return True
            if text in ("false", "0", "no", "off"):
                return False
            return fail(f"{label} must be true or false")

        if t == "select":
            for opt in field.options or []:
                if str(opt.value) == str(raw):
                    return opt.value
            return fail(f"{label}: '{raw}' is not one of the allowed options")

        if t == "multiselect":
            if not isinstance(raw, list):
                raw = [raw]
            allowed = {str(o.value): o.value for o in field.options or []}
            bad = [str(v) for v in raw if str(v) not in allowed]
            if bad:
                return fail(f"{label}: not allowed: {', '.join(bad)}")
            return [allowed[str(v)] for v in raw]

        if t == "date":
            return date.fromisoformat(str(raw).strip()[:10]).isoformat()

        if t == "datetime":
            return _parse_datetime(raw)

        if t == "user":
            key = _split_user(raw)
            if key is None or not all(key):
                return fail(f"{label}: pick a user from the list")
            try:
                known = users()
            except UserDirectoryError as err:
                return fail(str(err))
            if key not in known:
                return fail(f"{label}: '{key[0]}' ({key[1]}) is not a registered user")
            return {"user_id": key[0], "domain": key[1]}

        if t == "json":
            if isinstance(raw, str):
                return json.loads(raw)
            return raw

        if t == "list":
            if not isinstance(raw, list):
                return fail(f"{label} must be a list")
            items = []
            for index, item in enumerate(raw):
                if not isinstance(item, dict):
                    errors.append({"field": f"{path}.{index}", "message": f"{label}: invalid item"})
                    continue
                if all(_blank(sub, item.get(sub.name)) for sub in field.fields or [] if sub.type != "section"):
                    continue  # ignore completely blank rows
                built = _build_fields(field.fields or [], item, cfg, f"{path}.{index}.", errors, users)
                items.append(built)
            if field.min_items and len(items) < field.min_items:
                return fail(f"{label} needs at least {field.min_items} item(s)")
            if field.max_items is not None and len(items) > field.max_items:
                return fail(f"{label} allows at most {field.max_items} item(s)")
            return items
    except (TypeError, ValueError) as err:
        kind = {"date": "date", "datetime": "date/time", "json": "JSON"}.get(t, t)
        return fail(f"{label} is not a valid {kind}: {err}")

    return fail(f"{label}: unsupported field type '{t}'")


def _blank(field: FieldDef, raw: Any) -> bool:
    if field.type == "user":
        return _split_user(raw) is None
    return _is_empty(raw)


def _build_fields(
    fields: List[FieldDef],
    values: Dict[str, Any],
    cfg: FormConfig,
    prefix: str,
    errors: List[dict],
    users: UserLookup,
) -> dict:
    out: dict = {}
    for field in fields:
        if field.type == "section":
            continue  # a heading only - nothing to send
        path = f"{prefix}{field.name}"
        if field.auto and not field.editable:
            value: Any = resolve_auto(field.auto, cfg)
        elif field.hidden:
            value = copy.deepcopy(field.default)
        else:
            raw = values.get(field.name)
            if field.name not in values and field.default is not None:
                # omitted entirely (e.g. API caller) -> configured default
                raw = resolve_auto("now", cfg) if field.default == "now" else copy.deepcopy(field.default)
            if _blank(field, raw) and field.auto:
                value = resolve_auto(field.auto, cfg)
            elif _blank(field, raw):
                value = None
            else:
                value = _coerce(field, raw, cfg, path, errors, users)
                if value is _MISSING:
                    continue

        if _is_empty(value):
            if field.required:
                hint = " (auto value is empty - check the configuration)" if field.auto else ""
                errors.append({"field": path, "message": f"{field.display_label} is required{hint}"})
            continue
        if field.type == "user":
            out[field.user_id_key] = value["user_id"]
            out[field.domain_key] = value["domain"]
        else:
            out[field.name] = value
    return out


class Revision(BaseModel):
    """The entry version an update replaces (sent by the Entries page's Update button)."""

    model_config = ConfigDict(extra="forbid")
    entry_id: str = Field(..., min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_\-]+$")
    version: int = Field(..., ge=1)
    trial_uuid: str = Field(..., min_length=1, max_length=64, pattern=r"^[A-Za-z0-9_\-]+$")


def version_keys(revision: Optional[Revision]) -> dict:
    """Hidden keys linking the versions of one entry.

    entry_id       stable across all versions of an entry
    entry_version  1 for a new entry, previous + 1 for an update
    supersedes     trial uuid of the version this one replaces (updates only)
    """
    if revision is None:
        return {"entry_id": str(uuid.uuid4()), "entry_version": 1}
    return {
        "entry_id": revision.entry_id,
        "entry_version": revision.version + 1,
        "supersedes": revision.trial_uuid,
    }


def build_payload(
    form: FormDef,
    values: Optional[dict],
    cfg: FormConfig,
    users: Optional[UserLookup] = None,
    revision: Optional[Revision] = None,
) -> dict:
    """Build and validate the KV payload; raises SubmissionError with field errors.

    `users` returns the registered users for 'user' fields (default: the
    metadata store). It is called at most once per submission.
    `revision` makes this payload the next version of an existing entry.
    """
    errors: List[dict] = []
    source = users or (lambda: directory.lookup(cfg.users))
    cache: dict = {}

    def lookup() -> Dict[UserKey, dict]:
        if "users" not in cache:
            try:
                cache["users"] = source()
            except UserDirectoryError as err:
                cache["users"] = err
        if isinstance(cache["users"], Exception):
            raise cache["users"]
        return cache["users"]

    body = _build_fields(form.fields, values or {}, cfg, "", errors, lookup)
    if errors:
        raise SubmissionError(errors)

    # form_id and the version keys are never shown to the person filling in the
    # form; form_id tags every entry with the form it came from so it can be
    # found later (e.g. RWS type1 search_terms: ["powder-library"]).
    payload = {
        "schema_version": SCHEMA_VERSION,
        "msg_type": form.msg_type,
        "form_id": form.id,
        **version_keys(revision),
        **body,
    }
    errors = schema_errors(payload)
    if errors:
        raise SubmissionError(errors)
    return payload
