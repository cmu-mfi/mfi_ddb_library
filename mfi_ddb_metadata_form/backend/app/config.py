"""
Form configuration: loading, validation, env interpolation and hot reload.

Everything a deployer would want to change lives in one YAML file
(default: /app/config/form-config.yaml, override with FORM_CONFIG_PATH):

  app:       page title / subtitle
  adapter:   KV adapter instance name and default trial_id
  publish:   dry-run switch, timeouts
  history:   local submission log
  streamer:  mfi_ddb Streamer config (MQTT broker, enterprise/site, user)
  forms:     the forms to render, one per KV msg_type use-case

String values may reference environment variables as ${VAR} or ${VAR:-default}
so secrets (e.g. the MQTT password) can come from docker compose / .env.

The file is re-read whenever its modification time changes, so editing it on
a mounted volume takes effect on the next page load - no container restart.
If an edited file is invalid, the last good configuration keeps being served
and the error is reported through /health and /config.
"""

from __future__ import annotations

import copy
import logging
import os
import re
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, List, Literal, Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

logger = logging.getLogger(__name__)

DEFAULT_CONFIG_PATH = "/app/config/form-config.yaml"

MSG_TYPES = ("project", "user", "tp-tag", "data")
RESERVED_FIELD_NAMES = {"schema_version", "msg_type"}
FIELD_TYPES = Literal[
    "text",
    "textarea",
    "email",
    "number",
    "integer",
    "select",
    "multiselect",
    "checkbox",
    "date",
    "datetime",
    "list",
    "json",
]
AUTO_KEYWORDS = {"now", "uuid", "trial_id"}
AUTO_PREFIXES = ("streamer.", "env.")

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")
_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_\-]*$")
_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9_\-]*$")


# ---------------------------------------------------------------------------
# Config models
# ---------------------------------------------------------------------------


class OptionDef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    value: Any
    label: Optional[str] = None


class FieldDef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Key written into the KV payload.")
    label: Optional[str] = Field(None, description="Label shown in the UI (defaults to name).")
    type: FIELD_TYPES = "text"
    required: bool = False
    default: Any = Field(None, description="Initial value (or fixed value for hidden fields).")
    placeholder: Optional[str] = None
    help: Optional[str] = Field(None, description="Hint text shown under the input.")
    options: Optional[List[OptionDef]] = Field(None, description="Choices for select/multiselect.")
    hidden: bool = Field(False, description="Not shown; 'default' is always sent.")
    auto: Optional[str] = Field(
        None,
        description="Server-filled value: now | uuid | trial_id | streamer.<path> | env.<VAR>",
    )
    editable: bool = Field(
        False, description="For auto fields: let the user override the auto value."
    )
    pattern: Optional[str] = Field(None, description="Regex the (text) value must fully match.")
    min: Optional[float] = None
    max: Optional[float] = None
    fields: Optional[List["FieldDef"]] = Field(None, description="Sub-fields of a 'list' item.")
    min_items: int = 0
    max_items: Optional[int] = None
    item_label: Optional[str] = Field(None, description="Name of one list item, e.g. 'role'.")

    @field_validator("options", mode="before")
    @classmethod
    def _normalize_options(cls, value):
        if value is None:
            return value
        normalized = []
        for opt in value:
            if isinstance(opt, dict):
                normalized.append(opt)
            else:
                normalized.append({"value": opt, "label": str(opt)})
        return normalized

    @model_validator(mode="after")
    def _check(self):
        if not _NAME_PATTERN.match(self.name):
            raise ValueError(
                f"field name '{self.name}' must start with a letter/underscore and "
                "contain only letters, digits, '_' or '-'"
            )
        if self.name in RESERVED_FIELD_NAMES:
            raise ValueError(f"field name '{self.name}' is reserved and set automatically")
        if self.type in ("select", "multiselect") and not self.options:
            raise ValueError(f"field '{self.name}': type '{self.type}' needs 'options'")
        if self.type == "list":
            if not self.fields:
                raise ValueError(f"field '{self.name}': type 'list' needs 'fields'")
            _check_unique([f.name for f in self.fields], f"sub-field of '{self.name}'")
        elif self.fields:
            raise ValueError(f"field '{self.name}': 'fields' is only valid for type 'list'")
        if self.auto is not None and not (
            self.auto in AUTO_KEYWORDS or self.auto.startswith(AUTO_PREFIXES)
        ):
            raise ValueError(
                f"field '{self.name}': auto '{self.auto}' must be one of "
                f"{sorted(AUTO_KEYWORDS)} or start with {AUTO_PREFIXES}"
            )
        if self.pattern is not None:
            try:
                re.compile(self.pattern)
            except re.error as err:
                raise ValueError(f"field '{self.name}': invalid pattern: {err}") from None
        return self

    @property
    def display_label(self) -> str:
        return self.label or self.name


FieldDef.model_rebuild()


class FormDef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(..., description="URL-safe id, e.g. 'new-project'.")
    msg_type: Literal["project", "user", "tp-tag", "data"]
    title: str
    description: Optional[str] = None
    submit_label: str = "Submit"
    enabled: bool = True
    fields: List[FieldDef]
    # "config" = defined in the YAML file, "ui" = created with the form builder
    source: Literal["config", "ui"] = Field("config", exclude=True)

    @model_validator(mode="after")
    def _check(self):
        if not _ID_PATTERN.match(self.id):
            raise ValueError(f"form id '{self.id}' must be lowercase letters, digits, '_' or '-'")
        _check_unique([f.name for f in self.fields], f"field in form '{self.id}'")
        return self


class AppSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    title: str = "MFI DDB Metadata Form"
    subtitle: Optional[str] = None


class AdapterSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    adapter_name: str = "metadata_form"
    trial_id: str = Field(
        "mfi-ddb-metadata",
        description="trial_id for the KV adapter session when the payload has none.",
    )


class PublishSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    dry_run: bool = Field(False, description="Validate and log only; never touch the broker.")
    connect_timeout_seconds: float = Field(10, gt=0, le=120)
    settle_seconds: float = Field(
        1.0, ge=0, le=30, description="Wait after publishing before disconnecting."
    )
    publish_timeout_seconds: float = Field(60, gt=0, le=600)


class HistorySection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = True
    max_entries: int = Field(500, ge=1)


class BuilderSection(BaseModel):
    model_config = ConfigDict(extra="forbid")
    enabled: bool = Field(True, description="Allow creating forms in the browser.")
    admin_token: str = Field(
        "", description="If set, the builder asks for this token before saving/deleting."
    )


class FormConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    app: AppSection = AppSection()
    adapter: AdapterSection = AdapterSection()
    publish: PublishSection = PublishSection()
    history: HistorySection = HistorySection()
    builder: BuilderSection = BuilderSection()
    streamer: dict = Field(..., description="mfi_ddb Streamer config (user, mqtt, project).")
    forms: List[FormDef] = Field(..., min_length=1)

    @field_validator("streamer")
    @classmethod
    def _validate_streamer(cls, value: dict) -> dict:
        import mfi_ddb  # local import keeps module import cheap for tooling

        value = copy.deepcopy(value)
        topic_family = value.get("topic_family", "kv")
        if topic_family not in ("", "kv"):
            logger.warning("streamer.topic_family=%r ignored; this app always uses 'kv'", topic_family)
        value["topic_family"] = "kv"
        try:
            normalized = mfi_ddb.Streamer.SCHEMA(**value).model_dump(exclude_none=True)
        except ValidationError as err:
            raise ValueError(f"invalid streamer config: {err}") from None
        return normalized

    @model_validator(mode="after")
    def _check(self):
        _check_unique([f.id for f in self.forms], "form id")
        if not any(f.enabled for f in self.forms):
            raise ValueError("at least one form must be enabled")
        return self

    def get_form(self, form_id: str) -> Optional[FormDef]:
        for form in self.forms:
            if form.id == form_id and form.enabled:
                return form
        return None


def _check_unique(names: List[str], what: str) -> None:
    seen = set()
    for name in names:
        if name in seen:
            raise ValueError(f"duplicate {what}: '{name}'")
        seen.add(name)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def interpolate_env(value: Any) -> Any:
    """Replace ${VAR} / ${VAR:-default} in every string of a parsed YAML tree."""
    if isinstance(value, str):

        def _sub(match: re.Match) -> str:
            name, default = match.group(1), match.group(2)
            env_value = os.environ.get(name)
            if env_value is None or (env_value == "" and default is not None):
                return default if default is not None else ""
            return env_value

        return _ENV_PATTERN.sub(_sub, value)
    if isinstance(value, dict):
        return {k: interpolate_env(v) for k, v in value.items()}
    if isinstance(value, list):
        return [interpolate_env(v) for v in value]
    return value


def parse_config(raw: dict) -> FormConfig:
    if not isinstance(raw, dict):
        raise ValueError("configuration file must contain a YAML mapping")
    return FormConfig(**interpolate_env(raw))


def load_config_file(path: Path) -> FormConfig:
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    return parse_config(raw)


class ConfigManager:
    """Serves the latest valid config and reloads it when the file changes."""

    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path or os.environ.get("FORM_CONFIG_PATH", DEFAULT_CONFIG_PATH))
        self._lock = threading.Lock()
        self._config: Optional[FormConfig] = None
        self._mtime: Optional[float] = None
        self.error: Optional[str] = None

    def get(self) -> Optional[FormConfig]:
        with self._lock:
            try:
                mtime = self.path.stat().st_mtime
            except OSError as err:
                self.error = f"cannot read config file {self.path}: {err}"
                return self._config

            if mtime != self._mtime:
                self._mtime = mtime
                try:
                    self._config = load_config_file(self.path)
                    self.error = None
                    logger.info("Loaded form configuration from %s", self.path)
                except Exception as err:  # keep serving the last good config
                    self.error = f"invalid config file {self.path}: {err}"
                    logger.error(self.error)
            return self._config


# ---------------------------------------------------------------------------
# Auto values
# ---------------------------------------------------------------------------


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def resolve_auto(source: str, cfg: FormConfig) -> Any:
    if source == "now":
        return utc_now_iso()
    if source == "uuid":
        return str(uuid.uuid4())
    if source == "trial_id":
        return cfg.adapter.trial_id
    if source.startswith("env."):
        return os.environ.get(source[len("env."):])
    if source.startswith("streamer."):
        node: Any = cfg.streamer
        for part in source[len("streamer."):].split("."):
            if not isinstance(node, dict) or part not in node:
                return None
            node = node[part]
        if source.startswith("streamer.mqtt.password"):
            return None  # never leak the broker password into a payload
        return node
    return None


# ---------------------------------------------------------------------------
# Public view (what the browser is allowed to see)
# ---------------------------------------------------------------------------


def topic_prefix(cfg: FormConfig) -> str:
    mqtt = cfg.streamer.get("mqtt", {})
    parts = ["mfi-v1.0-kv", mqtt.get("enterprise", ""), mqtt.get("site", ""), mqtt.get("area", "")]
    return "/".join(p for p in parts if p)


def _public_field(field: FieldDef, cfg: FormConfig) -> dict:
    data = {
        "name": field.name,
        "label": field.display_label,
        "type": field.type,
        "required": field.required,
        "default": field.default,
        "placeholder": field.placeholder,
        "help": field.help,
        "options": [o.model_dump() for o in field.options] if field.options else None,
        "auto": field.auto,
        "editable": field.editable,
        "pattern": field.pattern,
        "min": field.min,
        "max": field.max,
        "min_items": field.min_items,
        "max_items": field.max_items,
        "item_label": field.item_label,
    }
    if field.auto and field.auto not in ("now", "uuid"):
        data["auto_value"] = resolve_auto(field.auto, cfg)
    if field.fields:
        data["fields"] = [_public_field(f, cfg) for f in field.fields if not f.hidden]
    return data


def public_view(cfg: FormConfig) -> dict:
    mqtt = cfg.streamer.get("mqtt", {})
    user = cfg.streamer.get("user", {})
    return {
        "app": cfg.app.model_dump(),
        "dry_run": cfg.publish.dry_run,
        "target": {
            "broker": f"{mqtt.get('broker_address', '')}:{mqtt.get('broker_port', 1883)}",
            "topic_prefix": topic_prefix(cfg),
        },
        "user": {k: user.get(k, "") for k in ("user_id", "domain", "name")},
        "history_enabled": cfg.history.enabled,
        "builder": {
            "enabled": cfg.builder.enabled,
            "requires_token": bool(cfg.builder.admin_token),
        },
        "forms": [
            {
                "id": form.id,
                "source": form.source,
                "msg_type": form.msg_type,
                "title": form.title,
                "description": form.description,
                "submit_label": form.submit_label,
                "fields": [_public_field(f, cfg) for f in form.fields if not f.hidden],
            }
            for form in cfg.forms
            if form.enabled
        ],
    }
