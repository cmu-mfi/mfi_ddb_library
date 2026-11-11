"""
Forms created in the browser (form builder).

Builder forms are simple 'data' forms: the user picks a title and a list of
keys (name, label, type, required, choices, ...). They are stored as JSON in
the data volume (FORM_CUSTOM_FORMS, default data/custom_forms.json) - the YAML
config file is never modified - and merged with the YAML forms at runtime.

Every builder form is converted to a regular FormDef:
    trial_id (required)  +  the user's keys
    +  created_by_user_id / created_by_domain (required, registered-user picker)
    +  timestamp (auto: now)
and published exactly like a YAML-defined form. The added fields are part of
every builder form automatically, so they cannot be used as key names.
"""

from __future__ import annotations

import json
import os
import re
import threading
from pathlib import Path
from typing import List, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.config import FieldDef, FormDef, check_topic_segment

BUILDER_TYPES = Literal[
    "text",
    "textarea",
    "email",
    "number",
    "integer",
    "checkbox",
    "select",
    "multiselect",  # multiple choice
    "date",
    "datetime",
    "person",  # registered-user picker: <key> = user ID, <key>_domain = domain
    "section",  # heading + description that groups the keys after it; no data
]
RESERVED_KEYS = {
    "schema_version",
    "msg_type",
    "trial_id",
    "timestamp",
    "created_by_user_id",
    "created_by_domain",
}


def created_by_fields() -> List[FieldDef]:
    """Who created the record: same fields as the YAML forms (and kv.json's
    user / project / tp-tag messages). Must be picked from the registered users
    (no pre-fill), and it is the user the publish session is recorded under."""
    return [
        FieldDef(
            name="created_by_user_id",
            label="Created by",
            required=True,
            lookup="user",
            domain_field="created_by_domain",
        ),
        FieldDef(
            name="created_by_domain",
            label="Created by (domain)",
            auto="streamer.user.domain",
            editable=True,
            required=True,
        ),
    ]
_KEY_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")


class BuilderKey(BaseModel):
    model_config = ConfigDict(extra="ignore")

    name: str = Field("", description="Key written into the payload (not used by sections)")
    label: Optional[str] = None
    type: BUILDER_TYPES = "text"
    required: bool = False
    options: List[str] = []
    default: Optional[str] = None
    placeholder: Optional[str] = None
    help: Optional[str] = None
    min: Optional[float] = None
    max: Optional[float] = None

    @field_validator("name")
    @classmethod
    def _name(cls, value: str) -> str:
        value = (value or "").strip()
        if value and not _KEY_PATTERN.match(value):
            raise ValueError(
                f"key '{value}' must start with a letter or '_' and contain only "
                "letters, digits, '_' or '-'"
            )
        if value in RESERVED_KEYS:
            raise ValueError(f"key '{value}' is added automatically; choose another name")
        return value

    @property
    def is_section(self) -> bool:
        return self.type == "section"

    def payload_keys(self) -> List[str]:
        """Keys this row writes into the payload."""
        if self.is_section:
            return []
        if self.type == "person":
            return [self.name, f"{self.name}_domain"]
        return [self.name]

    @field_validator("options")
    @classmethod
    def _options(cls, value: List[str]) -> List[str]:
        cleaned = [o.strip() for o in value if o and o.strip()]
        if len(set(cleaned)) != len(cleaned):
            raise ValueError("choices must be unique")
        return cleaned

    @model_validator(mode="after")
    def _check(self):
        if self.is_section:
            if not (self.label or "").strip():
                raise ValueError("a section needs a heading")
            self.name = ""
            return self
        if not self.name:
            raise ValueError(f"'{self.label or 'a field'}' needs a key name")
        if self.type in ("select", "multiselect") and not self.options:
            kind = "dropdown" if self.type == "select" else "multiple choice"
            raise ValueError(f"key '{self.name}': a {kind} needs at least one choice")
        if self.type == "person" and f"{self.name}_domain" in RESERVED_KEYS:
            raise ValueError(f"key '{self.name}' is added automatically; choose another name")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(f"key '{self.name}': min is greater than max")
        if self.default in ("",) or self.type in ("multiselect", "person"):
            self.default = None
        return self

    def to_fields(self) -> List[FieldDef]:
        label = (self.label or "").strip() or None
        help_text = (self.help or "").strip() or None
        if self.is_section:
            return [FieldDef(type="section", label=label, help=help_text)]
        if self.type == "person":
            return [
                FieldDef(
                    name=self.name,
                    label=label,
                    required=self.required,
                    placeholder=(self.placeholder or "").strip() or None,
                    help=help_text,
                    lookup="user",
                    domain_field=f"{self.name}_domain",
                ),
                FieldDef(
                    name=f"{self.name}_domain",
                    label=f"{label or self.name} (domain)",
                    required=self.required,
                ),
            ]
        default = self.default
        if self.type == "checkbox":
            default = str(default).lower() in ("true", "1", "yes", "on") if default else False
        is_number = self.type in ("number", "integer")
        has_choices = self.type in ("select", "multiselect")
        return [
            FieldDef(
                name=self.name,
                label=label,
                type=self.type,
                required=self.required,
                default=default,
                placeholder=(self.placeholder or "").strip() or None,
                help=help_text,
                options=(self.options or None) if has_choices else None,
                min=self.min if is_number else None,
                max=self.max if is_number else None,
            )
        ]


class BuilderForm(BaseModel):
    model_config = ConfigDict(extra="ignore")

    id: Optional[str] = None
    title: str = Field(..., min_length=1, max_length=80)
    description: Optional[str] = None
    site: Optional[str] = Field(None, description="Site to publish under (default: the app default).")
    keys: List[BuilderKey] = Field(..., min_length=1)

    @field_validator("site", mode="before")
    @classmethod
    def _site(cls, value):
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return check_topic_segment(value, "site")

    @model_validator(mode="after")
    def _check(self):
        names = [n for k in self.keys for n in k.payload_keys()]
        if not names:
            raise ValueError("add at least one key (sections alone hold no data)")
        dupes = sorted({n for n in names if names.count(n) > 1})
        if dupes:
            raise ValueError(f"duplicate key(s): {', '.join(dupes)}")
        self.title = self.title.strip()
        return self

    def to_form(self) -> FormDef:
        return FormDef(
            id=self.id,
            msg_type="data",
            title=self.title,
            description=(self.description or "").strip() or None,
            site=self.site,
            source="ui",
            fields=[
                FieldDef(
                    name="trial_id",
                    label="Trial ID",
                    required=True,
                    help="Data is attached to this trial.",
                ),
                *[f for k in self.keys for f in k.to_fields()],
                # With sections, give the automatic "Created by" its own heading
                # so it doesn't look like part of the user's last section.
                *(
                    [FieldDef(type="section", label="Submitted by")]
                    if any(k.is_section for k in self.keys)
                    else []
                ),
                *created_by_fields(),
                FieldDef(name="timestamp", auto="now"),
            ],
        )


def slugify(title: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", title.lower()).strip("-")
    return slug or "form"


class CustomFormStore:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path or os.environ.get("FORM_CUSTOM_FORMS", "data/custom_forms.json"))
        self._lock = threading.Lock()

    def _read(self) -> List[dict]:
        try:
            with open(self.path, encoding="utf-8") as fh:
                data = json.load(fh)
            return data if isinstance(data, list) else []
        except FileNotFoundError:
            return []

    def _write(self, items: List[dict]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(items, fh, indent=2)
        os.replace(tmp, self.path)

    def list(self) -> List[BuilderForm]:
        with self._lock:
            forms = []
            for item in self._read():
                try:
                    forms.append(BuilderForm(**item))
                except Exception:
                    continue  # skip anything hand-edited into an invalid state
            return forms

    def get(self, form_id: str) -> Optional[BuilderForm]:
        return next((f for f in self.list() if f.id == form_id), None)

    def save(self, form: BuilderForm, taken_ids: set, form_id: Optional[str] = None) -> BuilderForm:
        """Create (form_id None) or replace a builder form. Returns the stored form."""
        with self._lock:
            items = self._read()
            existing = {i.get("id") for i in items}
            if form_id is None:
                base = slugify(form.title)
                candidate, n = base, 2
                while candidate in existing or candidate in taken_ids:
                    candidate, n = f"{base}-{n}", n + 1
                form.id = candidate
                form.to_form()  # validate the generated definition before storing
                items.append(form.model_dump())
            else:
                if form_id not in existing:
                    raise KeyError(form_id)
                form.id = form_id
                form.to_form()
                items = [form.model_dump() if i.get("id") == form_id else i for i in items]
            self._write(items)
            return form

    def delete(self, form_id: str) -> bool:
        with self._lock:
            items = self._read()
            kept = [i for i in items if i.get("id") != form_id]
            if len(kept) == len(items):
                return False
            self._write(kept)
            return True
