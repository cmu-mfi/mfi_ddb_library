"""
Printable labels for form entries, using the label maker templates
(label_maker_main: blabel + WeasyPrint, label_template_S/L.html + styleS/L.css).

The Entries page sends the entries to print; each becomes one template record:

  file_id          short entry ID (title) - unique, stable across versions
  owner_displ      the created_by user's name, e.g. "D. Sinclair"
  <template var>   payload value, per the `labels.forms.<form_id>` mapping
  trial_id, entry_id, entry_version   always available to templates

All values are HTML-escaped (the templates are plain Jinja), so entry text can't
inject markup or resource URLs into the PDF renderer.
"""

from __future__ import annotations

import logging
import os
import re
import tempfile
from pathlib import Path
from typing import Dict, List, Optional

from markupsafe import escape
from pydantic import BaseModel, ConfigDict, Field

from app.config import FormConfig, LabelsSection

logger = logging.getLogger(__name__)


class LabelError(Exception):
    def __init__(self, message: str, status: int = 422):
        super().__init__(message)
        self.status = status


class LabelEntry(BaseModel):
    model_config = ConfigDict(extra="ignore")
    entry_id: str = Field(..., min_length=1, max_length=64)
    version: int = 1
    trial_id: Optional[str] = None
    values: Dict[str, object] = Field(default_factory=dict)


class LabelRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    form_id: str
    size: str
    entries: List[LabelEntry] = Field(..., min_length=1)


def short_entry_id(entry_id: str) -> str:
    """'e30717d6-722d-4dc0-...' -> 'e30717d6722d' (12 characters, like the label maker's file_id)."""
    return re.sub(r"[^A-Za-z0-9]", "", entry_id)[:12]


def owner_display(user_id: Optional[str], name: Optional[str]) -> str:
    """'Daniel Sinclair' -> 'D. Sinclair'; no name -> the user id."""
    parts = (name or "").split()
    if len(parts) >= 2:
        return f"{parts[0][0]}. {' '.join(parts[1:])}"
    return parts[0] if parts else (user_id or "")


def _text(value) -> str:
    if value is None:
        return ""
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(escape(str(value)))


def label_record(mapping: Dict[str, str], entry: LabelEntry, users: Dict[tuple, dict]) -> dict:
    values = entry.values
    user_id, domain = values.get("created_by_user_id"), values.get("created_by_domain")
    user = users.get((user_id, domain)) or {}
    record = {var: _text(values.get(key)) for var, key in mapping.items()}
    record.update(
        file_id=_text(short_entry_id(entry.entry_id)),
        owner_displ=_text(owner_display(user_id, user.get("name"))),
        trial_id=_text(entry.trial_id or values.get("trial_id")),
        entry_id=_text(entry.entry_id),
        entry_version=_text(entry.version),
    )
    return record


def check_templates(labels: LabelsSection) -> Optional[str]:
    """None if every configured template/stylesheet exists, else what is missing."""
    if not labels.enabled or not labels.forms:
        return None
    base = Path(labels.template_dir)
    missing = [
        name
        for size in labels.sizes.values()
        for name in (size.template, size.stylesheet)
        if name and not (base / name).is_file()
    ]
    if missing:
        return f"label templates not found in {base}: {', '.join(missing)}"
    return None


def labels_view(cfg: FormConfig) -> dict:
    """What the browser needs: which forms can print and in which sizes."""
    ok = cfg.labels.enabled and check_templates(cfg.labels) is None
    return {
        "sizes": list(cfg.labels.sizes) if ok else [],
        "forms": list(cfg.labels.forms) if ok else [],
    }


def render_labels(cfg: FormConfig, request: LabelRequest, users: Dict[tuple, dict]) -> bytes:
    labels = cfg.labels
    if not labels.enabled:
        raise LabelError("Label printing is disabled in the configuration.", 403)
    mapping = labels.forms.get(request.form_id)
    if mapping is None:
        raise LabelError(f"Form '{request.form_id}' has no label configured.", 404)
    size = labels.sizes.get(request.size)
    if size is None:
        raise LabelError(f"Unknown label size '{request.size}' (use {', '.join(labels.sizes)}).")
    if len(request.entries) > labels.max_labels:
        raise LabelError(f"At most {labels.max_labels} labels per print.")
    problem = check_templates(labels)
    if problem:
        raise LabelError(problem, 503)

    from blabel import LabelWriter  # heavy import (WeasyPrint); only when printing

    base = Path(labels.template_dir).resolve()
    writer = LabelWriter(
        str(base / size.template),
        default_stylesheets=(str(base / size.stylesheet),) if size.stylesheet else (),
        default_base_url=str(base),
        encoding="utf-8",
    )
    records = [label_record(mapping, e, users) for e in request.entries]

    # One label per page: the label templates style `body` as a flex row, which
    # would put several labels side by side on one page and ignore blabel's page
    # breaks. Override that, and make the page the size of the label.
    from weasyprint import CSS

    page_rule = f"@page {{ size: {size.page_size}; margin: 0 }}" if size.page_size else ""
    layout = CSS(string=f"{page_rule} html, body {{ display: block !important; min-height: 0 !important; }}")

    fd, target = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    try:
        writer.write_labels(records, target=target, extra_stylesheets=(layout,))
        return Path(target).read_bytes()
    finally:
        try:
            os.unlink(target)
        except OSError:
            pass
