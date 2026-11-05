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
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from app.config import FieldDef, FormConfig, FormDef, resolve_auto

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
        provided = {f.name for f in form.fields if not f.hidden or f.default is not None}
        guaranteed = {
            f.name
            for f in form.fields
            if f.required or (f.auto and not f.editable) or (f.hidden and f.default is not None)
        }
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


def _coerce(field: FieldDef, raw: Any, cfg: FormConfig, path: str, errors: List[dict]) -> Any:
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
                if all(_is_empty(item.get(sub.name)) for sub in field.fields or []):
                    continue  # ignore completely blank rows
                built = _build_fields(field.fields or [], item, cfg, f"{path}.{index}.", errors)
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


def _build_fields(
    fields: List[FieldDef], values: Dict[str, Any], cfg: FormConfig, prefix: str, errors: List[dict]
) -> dict:
    out: dict = {}
    for field in fields:
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
            if _is_empty(raw) and field.auto:
                value = resolve_auto(field.auto, cfg)
            elif _is_empty(raw):
                value = None
            else:
                value = _coerce(field, raw, cfg, path, errors)
                if value is _MISSING:
                    continue

        if _is_empty(value):
            if field.required:
                hint = " (auto value is empty - check the configuration)" if field.auto else ""
                errors.append({"field": path, "message": f"{field.display_label} is required{hint}"})
            continue
        out[field.name] = value
    return out


def build_payload(form: FormDef, values: Optional[dict], cfg: FormConfig) -> dict:
    """Build and validate the KV payload; raises SubmissionError with field errors."""
    errors: List[dict] = []
    body = _build_fields(form.fields, values or {}, cfg, "", errors)
    if errors:
        raise SubmissionError(errors)

    payload = {"schema_version": SCHEMA_VERSION, "msg_type": form.msg_type, **body}
    errors = schema_errors(payload)
    if errors:
        raise SubmissionError(errors)
    return payload
