"""
Settings edited in the browser (Settings page).

Lets an admin change, without touching the YAML file:
  * the MQTT target: broker address/port, enterprise, username/password
  * the site each form publishes under (per form)

Overrides are stored as JSON in the data volume (FORM_SETTINGS, default
data/settings.json) and layered on top of the YAML configuration at runtime:

    YAML streamer.mqtt  <-  settings.mqtt        (non-empty values win)
    form.site / YAML default site  <-  settings.form_sites[form_id]

Removing an override (empty value or "Reset to defaults") falls back to YAML.
The broker password is write-only: it is never returned by the API.
"""

from __future__ import annotations

import copy
import json
import os
import threading
from pathlib import Path
from typing import Dict, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator

from app.config import FormConfig, check_topic_segment

MQTT_KEYS = ("broker_address", "broker_port", "enterprise", "username", "password")


class MqttSettings(BaseModel):
    """MQTT overrides. None = use the YAML value."""

    model_config = ConfigDict(extra="forbid")

    broker_address: Optional[str] = None
    broker_port: Optional[int] = Field(None, ge=1, le=65535)
    enterprise: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None

    @field_validator("broker_address", "username", "password", mode="before")
    @classmethod
    def _blank_is_none(cls, value):
        if isinstance(value, str) and not value.strip():
            return None
        return value

    @field_validator("broker_address")
    @classmethod
    def _address(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = value.strip()
        if any(c.isspace() for c in value) or "://" in value:
            raise ValueError("broker address must be a host name or IP, e.g. mqtt-broker or 10.0.0.5")
        return value

    @field_validator("broker_port", mode="before")
    @classmethod
    def _port(cls, value):
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return value

    @field_validator("enterprise", mode="before")
    @classmethod
    def _enterprise(cls, value):
        if value is None or (isinstance(value, str) and not value.strip()):
            return None
        return check_topic_segment(value, "enterprise")


class Settings(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mqtt: MqttSettings = MqttSettings()
    form_sites: Dict[str, str] = Field(default_factory=dict)

    @field_validator("form_sites", mode="before")
    @classmethod
    def _sites(cls, value):
        if not isinstance(value, dict):
            raise ValueError("form_sites must be a mapping of form id to site")
        cleaned = {}
        for form_id, site in value.items():
            if site is None or (isinstance(site, str) and not site.strip()):
                continue  # empty = use the form's default site
            cleaned[str(form_id)] = check_topic_segment(site, f"site of form '{form_id}'")
        return cleaned


class SettingsUpdate(BaseModel):
    """Body of PUT /settings.

    mqtt.password: omitted or null keeps the stored password, "" clears it.
    Any other empty value removes that override (back to the YAML default).
    """

    model_config = ConfigDict(extra="forbid")

    mqtt: Dict[str, object] = Field(default_factory=dict)
    form_sites: Dict[str, Optional[str]] = Field(default_factory=dict)


def apply_settings(cfg: FormConfig, settings: Settings) -> FormConfig:
    """Return a copy of cfg with the browser overrides applied."""
    streamer = copy.deepcopy(cfg.streamer)
    mqtt = streamer.setdefault("mqtt", {})
    for key in MQTT_KEYS:
        value = getattr(settings.mqtt, key)
        if value is not None:
            mqtt[key] = value

    forms = [
        form.model_copy(update={"site": settings.form_sites[form.id]})
        if form.id in settings.form_sites
        else form
        for form in cfg.forms
    ]
    return cfg.model_copy(update={"streamer": streamer, "forms": forms})


class SettingsStore:
    def __init__(self, path: Optional[Path] = None) -> None:
        self.path = Path(path or os.environ.get("FORM_SETTINGS", "data/settings.json"))
        self._lock = threading.Lock()

    def get(self) -> Settings:
        with self._lock:
            return self._read()

    def _read(self) -> Settings:
        try:
            with open(self.path, encoding="utf-8") as fh:
                return Settings(**json.load(fh))
        except FileNotFoundError:
            return Settings()
        except Exception:
            # A hand-edited, broken file must not take the app down.
            return Settings()

    def _write(self, settings: Settings) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(settings.model_dump(exclude_none=True), fh, indent=2)
        os.replace(tmp, self.path)

    def update(self, update: SettingsUpdate) -> Settings:
        with self._lock:
            current = self._read()
            mqtt = current.mqtt.model_dump()
            for key, value in update.mqtt.items():
                if key not in MQTT_KEYS:
                    raise ValueError(f"unknown MQTT setting '{key}'")
                if key == "password" and value is None:
                    continue  # keep the stored password
                mqtt[key] = value
            sites = dict(current.form_sites)
            for form_id, site in update.form_sites.items():
                sites[form_id] = site
            settings = Settings(mqtt=MqttSettings(**mqtt), form_sites=sites)
            self._write(settings)
            return settings

    def reset(self) -> None:
        with self._lock:
            try:
                self.path.unlink()
            except FileNotFoundError:
                pass
