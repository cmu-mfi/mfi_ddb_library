"""
Batch add: many entries of a form from one table (CSV, Excel, or cells pasted
from a spreadsheet).

The first non-empty row holds the column titles - a field's key ("alloy") or
its label ("Alloy"), case-insensitive. Every following non-blank row is one
entry. "Created by" is picked once for the whole batch.

Checking is all or nothing: the table is parsed and every row is built with
the same `build_payload` a single submission uses (required values, dropdown
choices, number limits, dates, kv schema ...). Nothing is published here - the
browser publishes the rows one by one through /forms/{id}/submit once the
whole table is clean.
"""

from __future__ import annotations

import base64
import csv
import io
import re
from datetime import date, datetime, time
from typing import Any, Dict, List, Optional, Tuple

from pydantic import BaseModel, ConfigDict, Field

from app.config import RESERVED_FIELD_NAMES, FieldDef, FormConfig, FormDef
from app.payload import SubmissionError, build_payload
from app.users import UserDirectoryError, UserKey, directory

MAX_CONTENT_CHARS = 3_000_000  # ~2 MB of file data (xlsx arrives base64-encoded)
UNSUPPORTED_TYPES = {"list", "json", "multiselect"}
BATCH_USER_FIELD = "created_by"


class BatchError(Exception):
    def __init__(self, message: str, status: int = 422):
        super().__init__(message)
        self.status = status


class BatchUser(BaseModel):
    user_id: str = Field(..., min_length=1)
    domain: str = Field(..., min_length=1)


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    format: str = Field(..., pattern="^(csv|tsv|xlsx)$")
    content: str = Field(..., max_length=MAX_CONTENT_CHARS)
    created_by: Optional[BatchUser] = None


# ---------------------------------------------------------------------------
# parsing
# ---------------------------------------------------------------------------


def _cell(value: Any) -> Any:
    """Spreadsheet cell -> what a form would send (text, or a number)."""
    if value is None:
        return ""
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, datetime):
        return value.date().isoformat() if value.time() == time(0) else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, (int, float)):
        return value
    return str(value).strip()


def _read_text(fmt: str, content: str) -> List[List[Any]]:
    text = content.lstrip("﻿")
    if fmt == "tsv":
        delimiter = "\t"
    else:
        try:
            delimiter = csv.Sniffer().sniff(text[:4096], delimiters=",;\t").delimiter
        except csv.Error:
            delimiter = ","
    return [[_cell(c) for c in row] for row in csv.reader(io.StringIO(text), delimiter=delimiter)]


def _read_xlsx(content: str) -> List[List[Any]]:
    from openpyxl import load_workbook

    try:
        data = base64.b64decode(content, validate=True)
    except (ValueError, TypeError):
        raise BatchError("The Excel file could not be read (invalid upload).") from None
    try:
        workbook = load_workbook(io.BytesIO(data), read_only=True, data_only=True)
    except Exception:
        raise BatchError("This doesn't look like an Excel .xlsx file.") from None
    try:
        sheet = workbook.worksheets[0]
        return [[_cell(c) for c in row] for row in sheet.iter_rows(values_only=True)]
    finally:
        workbook.close()


def parse_table(fmt: str, content: str) -> Tuple[List[str], List[Tuple[int, List[Any]]]]:
    """-> (headers, [(spreadsheet row number, cells)]) ; blank rows dropped."""
    rows = _read_xlsx(content) if fmt == "xlsx" else _read_text(fmt, content)
    numbered = [(i + 1, row) for i, row in enumerate(rows)]
    numbered = [(n, row) for n, row in numbered if any(c != "" for c in row)]
    if not numbered:
        raise BatchError("The table is empty.")
    _, header_row = numbered[0]
    headers = [str(h).strip() for h in header_row]
    while headers and headers[-1] == "":
        headers.pop()
    return headers, numbered[1:]


# ---------------------------------------------------------------------------
# columns
# ---------------------------------------------------------------------------


def batch_fields(form: FormDef) -> List[FieldDef]:
    """Fields a table column can fill (Created by is picked once for the batch)."""
    return [
        f for f in form.fields
        if f.type != "section"
        and f.name != BATCH_USER_FIELD
        and not f.hidden
        and not (f.auto and not f.editable)
        and f.type not in UNSUPPORTED_TYPES
    ]


def _title(text: str) -> str:
    """'Date Received', 'date received', 'date_received' -> 'date_received'."""
    return re.sub(r"[^a-z0-9]+", "_", text.strip().lower()).strip("_")


def map_columns(form: FormDef, headers: List[str]) -> Tuple[List[Optional[FieldDef]], List[dict]]:
    """Column index -> field (None = ignored blank column) and file-level errors."""
    fillable = batch_fields(form)
    by_title: Dict[str, FieldDef] = {}
    for f in fillable:
        by_title.setdefault(_title(f.name), f)
        by_title.setdefault(_title(f.display_label), f)
    other = {_title(f.name): f for f in form.fields} | {_title(f.display_label): f for f in form.fields}

    columns: List[Optional[FieldDef]] = []
    errors: List[dict] = []
    seen: Dict[str, str] = {}
    for header in headers:
        key = _title(header)
        field = by_title.get(key)
        if header == "":
            columns.append(None)
            continue
        if field is None:
            known = other.get(key)
            if known is not None and known.name == BATCH_USER_FIELD:
                why = "is chosen once for the whole batch with the Created by picker"
            elif known is not None and (known.auto or known.hidden):
                why = "is filled in automatically"
            elif known is not None and known.type in UNSUPPORTED_TYPES:
                why = f"is a {known.type} field, which can't be filled from a table"
            elif key in RESERVED_FIELD_NAMES:
                why = "is filled in automatically"
            else:
                why = "doesn't match any field of this form"
            errors.append({"field": None, "message": f"Column '{header}' {why}."})
            columns.append(None)
            continue
        if field.name in seen:
            errors.append({
                "field": None,
                "message": f"Columns '{seen[field.name]}' and '{header}' both fill {field.display_label}.",
            })
            columns.append(None)
            continue
        seen[field.name] = header
        columns.append(field)

    for f in fillable:
        if f.required and f.name not in seen:
            errors.append({
                "field": None,
                "message": f"Missing required column: {f.display_label} ({f.name}).",
            })
    return columns, errors


# ---------------------------------------------------------------------------
# check
# ---------------------------------------------------------------------------


def check_batch(cfg: FormConfig, form: FormDef, request: BatchRequest) -> dict:
    headers, rows = parse_table(request.format, request.content)
    columns, errors = map_columns(form, headers)

    max_rows = cfg.batch.max_rows
    if len(rows) > max_rows:
        errors.append({"field": None, "message": f"{len(rows)} rows - at most {max_rows} per batch."})
    if not rows:
        errors.append({"field": None, "message": "The table has column titles but no rows."})

    # one user lookup for the whole batch
    users: Optional[Dict[UserKey, dict]] = None
    try:
        users = directory.lookup(cfg.users)
    except UserDirectoryError as err:
        errors.append({"field": None, "message": str(err)})

    created_by = request.created_by
    has_user_field = any(f.name == BATCH_USER_FIELD and f.type == "user" for f in form.fields)
    if has_user_field:
        if created_by is None:
            errors.append({"field": None, "message": "Pick who is adding these entries under Created by."})
        elif users is not None and (created_by.user_id, created_by.domain) not in users:
            errors.append({
                "field": None,
                "message": f"Created by: '{created_by.user_id}' ({created_by.domain}) is not a registered user.",
            })

    mapped = [(i, f) for i, f in enumerate(columns) if f is not None]
    checked = []
    if users is not None and len(rows) <= max_rows:
        for number, cells in rows:
            values = {f.name: (cells[i] if i < len(cells) else "") for i, f in mapped}
            if has_user_field and created_by is not None:
                values[BATCH_USER_FIELD] = created_by.model_dump()
            row_errors: List[dict] = []
            extra = [c for c in cells[len(headers):] if c != ""]
            if extra:
                row_errors.append({"field": None, "message": "Has values in columns without a title."})
            try:
                build_payload(form, values, cfg, users=lambda: users)
            except SubmissionError as err:
                row_errors.extend(
                    e for e in err.errors
                    # file-level problems (Created by, missing columns) are reported once above
                    if e.get("field") != BATCH_USER_FIELD
                    and not (e.get("field") and e["field"] not in values)
                )
            checked.append({"row": number, "values": values, "errors": row_errors})

    bad_rows = sum(1 for r in checked if r["errors"])
    return {
        "ok": not errors and bad_rows == 0 and bool(checked),
        "columns": [
            {"header": h, "field": f.name if f else None, "label": f.display_label if f else None}
            for h, f in zip(headers, columns)
        ],
        "rows": checked,
        "errors": errors,
        "count": len(checked),
        "bad_rows": bad_rows,
    }
