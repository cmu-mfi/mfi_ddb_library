import copy
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app import config as config_module
from app import publisher
from app.config import ConfigManager, parse_config, resolve_auto
from app.payload import SubmissionError, build_payload, lint_config

_REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "form-config.yaml"
DEFAULT_CONFIG = _REPO_CONFIG if _REPO_CONFIG.exists() else Path("/app/config/form-config.yaml")


@pytest.fixture
def raw_config():
    with open(DEFAULT_CONFIG, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture
def cfg(raw_config):
    return parse_config(raw_config)


# --------------------------------------------------------------------------- config


def test_default_config_is_valid_and_lint_clean(cfg):
    assert [f.id for f in cfg.forms if f.enabled] == ["project", "user", "trial-tag"]
    assert cfg.streamer["topic_family"] == "kv"
    assert lint_config(cfg) == []


def test_env_interpolation(raw_config, monkeypatch):
    monkeypatch.setenv("MQTT_BROKER_ADDRESS", "mfi-mqtt-broker")
    monkeypatch.setenv("MQTT_BROKER_PORT", "1884")
    monkeypatch.setenv("FORM_DRY_RUN", "true")
    cfg = parse_config(raw_config)
    assert cfg.streamer["mqtt"]["broker_address"] == "mfi-mqtt-broker"
    assert cfg.streamer["mqtt"]["broker_port"] == 1884
    assert cfg.publish.dry_run is True


def test_typo_in_field_option_is_rejected(raw_config):
    raw_config["forms"][0]["fields"][0]["requried"] = True
    with pytest.raises(Exception, match="requried"):
        parse_config(raw_config)


def test_select_without_options_is_rejected(raw_config):
    raw_config["forms"][0]["fields"].append({"name": "x", "type": "select"})
    with pytest.raises(Exception, match="needs 'options'"):
        parse_config(raw_config)


def test_lint_flags_missing_schema_field(raw_config):
    form = raw_config["forms"][1]  # user form
    form["fields"] = [f for f in form["fields"] if f["name"] != "domain"]
    warnings = lint_config(parse_config(raw_config))
    assert any("'domain'" in w for w in warnings)


def test_auto_never_returns_password(cfg):
    cfg.streamer["mqtt"]["password"] = "secret"
    assert resolve_auto("streamer.mqtt.password", cfg) is None
    assert resolve_auto("streamer.user.user_id", cfg) == cfg.streamer["user"]["user_id"]


def test_config_manager_keeps_last_good_config(tmp_path, raw_config):
    path = tmp_path / "form-config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    manager = ConfigManager(path)
    assert manager.get() is not None

    import os, time

    path.write_text("forms: [ this is : not valid")
    os.utime(path, (time.time() + 5, time.time() + 5))
    assert manager.get() is not None
    assert manager.error


# --------------------------------------------------------------------------- payloads


def test_project_payload(cfg):
    form = cfg.get_form("project")
    payload = build_payload(
        form,
        {
            "project_id": "p-550",
            "project_name": "Apollo",
            "user_roles": [
                {"user_id": "jsmith", "domain": "ANDREW", "role": "admin"},
                {"user_id": "", "domain": "", "role": ""},  # blank row ignored
            ],
        },
        cfg,
    )
    assert payload["schema_version"] == "mfi-v1.0"
    assert payload["msg_type"] == "project"
    assert payload["user_roles"] == [{"user_id": "jsmith", "domain": "ANDREW", "role": "admin"}]
    assert payload["created_by_user_id"] == cfg.streamer["user"]["user_id"]
    assert payload["timestamp"].endswith("Z")


def test_required_and_pattern_errors(cfg):
    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("project"), {"project_id": "has spaces"}, cfg)
    assert exc.value.errors[0]["field"] == "project_id"

    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("user"), {"domain": "ANDREW"}, cfg)
    assert {e["field"] for e in exc.value.errors} == {"user_id"}


def test_nested_list_errors_have_paths(cfg):
    with pytest.raises(SubmissionError) as exc:
        build_payload(
            cfg.get_form("project"),
            {"project_id": "p1", "user_roles": [{"user_id": "a", "domain": "D", "role": "boss"}]},
            cfg,
        )
    assert exc.value.errors[0]["field"] == "user_roles.0.role"


def test_tp_tag_datetimes_normalized(cfg):
    payload = build_payload(
        cfg.get_form("trial-tag"),
        {
            "trial_id": "t-1",
            "project_id": "p-1",
            "time_start": "2026-11-05T10:00:00-05:00",
            "time_end": "2026-11-05T16:30:00.000Z",
            "trial_user_id": "op",
            "trial_user_domain": "ANDREW",
        },
        cfg,
    )
    assert payload["time_start"] == "2026-11-05T15:00:00Z"
    assert payload["time_end"] == "2026-11-05T16:30:00Z"


def test_editable_auto_can_be_overridden(cfg):
    payload = build_payload(
        cfg.get_form("user"),
        {"user_id": "u", "domain": "D", "created_by_user_id": "someone-else"},
        cfg,
    )
    assert payload["created_by_user_id"] == "someone-else"


def test_non_editable_auto_cannot_be_spoofed(cfg):
    payload = build_payload(
        cfg.get_form("user"), {"user_id": "u", "domain": "D", "timestamp": "1999-01-01"}, cfg
    )
    assert not payload["timestamp"].startswith("1999")


def test_kv_schema_any_of_message(raw_config):
    # A tp-tag form without project fields can only fail the kv schema's anyOf.
    form = raw_config["forms"][2]
    form["fields"] = [f for f in form["fields"] if f["name"] != "project_id"]
    cfg = parse_config(raw_config)
    with pytest.raises(SubmissionError) as exc:
        build_payload(
            cfg.get_form("trial-tag"),
            {
                "trial_id": "t",
                "time_start": "2026-01-01T00:00:00Z",
                "time_end": "2026-01-01T01:00:00Z",
                "trial_user_id": "op",
                "trial_user_domain": "D",
            },
            cfg,
        )
    assert "at least one of: project_id, project_name" in exc.value.errors[0]["message"]


def test_custom_data_form(raw_config):
    next(f for f in raw_config["forms"] if f["id"] == "trial-note")["enabled"] = True
    cfg = parse_config(raw_config)
    payload = build_payload(
        cfg.get_form("trial-note"),
        {"trial_id": "t-9", "note": "ok", "temperature_c": "21.5", "passed_qc": True},
        cfg,
    )
    assert payload["temperature_c"] == 21.5 and payload["passed_qc"] is True
    with pytest.raises(SubmissionError):
        build_payload(cfg.get_form("trial-note"), {"trial_id": "t", "note": "x", "temperature_c": 500}, cfg)


# --------------------------------------------------------------------------- publishing


def test_adapter_config_uses_payload_trial_id(cfg):
    assert publisher.build_adapter_config({"msg_type": "tp-tag", "trial_id": "t-1"}, cfg)["trial_id"] == "t-1"
    assert publisher.build_adapter_config({"msg_type": "user"}, cfg)["trial_id"] == cfg.adapter.trial_id


def test_topic(cfg):
    assert publisher.topic_for(cfg, "project") == "mfi-v1.0-kv/CMU/Mill-19/project/data"


def test_publish_goes_through_kv_adapter_and_streamer(cfg, monkeypatch):
    import mfi_ddb

    calls = {}

    class FakeStreamer:
        def __init__(self, streamer_cfg, adapter, stream_on_update=False):
            calls["streamer_cfg"] = streamer_cfg
            calls["adapter"] = adapter
            adapter.get_data()
            calls["data"] = copy.deepcopy(adapter.data)

        def disconnect(self):
            calls["disconnected"] = True

    monkeypatch.setattr(mfi_ddb, "Streamer", FakeStreamer)
    monkeypatch.setattr(publisher, "check_broker", lambda cfg, timeout=None: None)
    cfg.publish.settle_seconds = 0

    payload = build_payload(cfg.get_form("user"), {"user_id": "u1", "domain": "ANDREW"}, cfg)
    result = publisher.publish(payload, cfg)

    assert result["status"] == "published"
    assert calls["streamer_cfg"]["topic_family"] == "kv"
    assert type(calls["adapter"]).__name__ == "KvDataAdapter"
    assert calls["data"]["user"]["user_id"] == "u1"
    assert calls["disconnected"]


def test_unreachable_broker_fails_fast(cfg):
    cfg.streamer["mqtt"]["broker_address"] = "127.0.0.1"
    cfg.streamer["mqtt"]["broker_port"] = 1  # nothing listens here
    with pytest.raises(publisher.PublishError):
        publisher.check_broker(cfg, timeout=2)


# --------------------------------------------------------------------------- API


@pytest.fixture
def client(tmp_path, raw_config, monkeypatch):
    raw_config["publish"]["dry_run"] = True
    path = tmp_path / "form-config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    monkeypatch.setenv("FORM_CONFIG_PATH", str(path))
    monkeypatch.setenv("FORM_HISTORY_DB", str(tmp_path / "history.db"))
    monkeypatch.setenv("FORM_CUSTOM_FORMS", str(tmp_path / "custom_forms.json"))

    import importlib

    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def test_api_flow(client):
    view = client.get("/config").json()
    assert [f["id"] for f in view["forms"]] == ["project", "user", "trial-tag"]
    assert "password" not in str(view)

    bad = client.post("/forms/user/submit", json={"values": {}})
    assert bad.status_code == 422
    assert {e["field"] for e in bad.json()["errors"]} == {"user_id"}

    ok = client.post("/forms/user/submit", json={"values": {"user_id": "u1", "domain": "ANDREW"}})
    assert ok.status_code == 200, ok.text
    assert ok.json()["status"] == "dry_run"

    history = client.get("/submissions").json()
    assert history[0]["form_id"] == "user" and history[0]["status"] == "dry_run"

    assert client.post("/forms/nope/submit", json={"values": {}}).status_code == 404


# --------------------------------------------------------------------------- form builder

MACHINE_FORM = {
    "title": "Machine setup",
    "description": "CNC settings",
    "keys": [
        {"name": "machine", "label": "Machine", "type": "select", "required": True,
         "options": ["Haas VF-2", "Prusa MK4"]},
        {"name": "spindle_rpm", "label": "Spindle RPM", "type": "integer", "min": 0, "max": 12000},
        {"name": "coolant_on", "label": "Coolant on", "type": "checkbox"},
        {"name": "operator", "label": "Operator"},
    ],
}


def test_builder_create_fill_publish_edit_delete(client):
    created = client.post("/builder/forms", json=MACHINE_FORM)
    assert created.status_code == 200, created.text
    form_id = created.json()["form"]["id"]
    assert form_id == "machine-setup"

    view = client.get("/config").json()
    form = next(f for f in view["forms"] if f["id"] == form_id)
    assert form["source"] == "ui" and form["msg_type"] == "data"
    assert [f["name"] for f in form["fields"]] == [
        "trial_id", "machine", "spindle_rpm", "coolant_on", "operator", "timestamp"
    ]

    # The person filling in the form supplies only the values.
    sent = client.post(
        f"/forms/{form_id}/submit",
        json={"values": {"trial_id": "t-1", "machine": "Haas VF-2", "spindle_rpm": "8000",
                         "coolant_on": True, "operator": "jdoe"}},
    )
    assert sent.status_code == 200, sent.text
    payload = sent.json()["payload"]
    assert payload["msg_type"] == "data"
    assert payload["machine"] == "Haas VF-2" and payload["spindle_rpm"] == 8000
    assert payload["coolant_on"] is True and payload["timestamp"].endswith("Z")

    bad = client.post(f"/forms/{form_id}/submit",
                      json={"values": {"trial_id": "t", "machine": "Lathe", "spindle_rpm": "99999"}})
    assert {e["field"] for e in bad.json()["errors"]} == {"machine", "spindle_rpm"}

    edited = dict(MACHINE_FORM, keys=MACHINE_FORM["keys"][:1])
    assert client.put(f"/builder/forms/{form_id}", json=edited).status_code == 200
    form = next(f for f in client.get("/config").json()["forms"] if f["id"] == form_id)
    assert [f["name"] for f in form["fields"]] == ["trial_id", "machine", "timestamp"]

    assert client.delete(f"/builder/forms/{form_id}").status_code == 200
    assert form_id not in [f["id"] for f in client.get("/config").json()["forms"]]


def test_builder_validation_messages(client):
    resp = client.post("/builder/forms", json={
        "title": "Bad", "keys": [
            {"name": "has space"}, {"name": "trial_id"}, {"name": "pick", "type": "select"}
        ]})
    assert resp.status_code == 422
    messages = " | ".join(e["message"] for e in resp.json()["errors"])
    assert "has space" in messages and "added automatically" in messages and "choice" in messages

    dupes = client.post("/builder/forms", json={"title": "D", "keys": [{"name": "a"}, {"name": "a"}]})
    assert "duplicate" in dupes.json()["errors"][0]["message"]


def test_builder_id_does_not_clash_with_yaml_forms(client):
    resp = client.post("/builder/forms", json={"title": "Project", "keys": [{"name": "x"}]})
    assert resp.json()["form"]["id"] == "project-2"
    assert client.put("/builder/forms/project", json={"title": "x", "keys": [{"name": "x"}]}).status_code == 404


def test_builder_admin_token(tmp_path, raw_config, monkeypatch):
    raw_config["builder"] = {"enabled": True, "admin_token": "s3cret"}
    path = tmp_path / "form-config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    monkeypatch.setenv("FORM_CONFIG_PATH", str(path))
    monkeypatch.setenv("FORM_HISTORY_DB", str(tmp_path / "history.db"))
    monkeypatch.setenv("FORM_CUSTOM_FORMS", str(tmp_path / "custom_forms.json"))

    import importlib

    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        assert c.get("/config").json()["builder"]["requires_token"] is True
        assert c.post("/builder/forms", json=MACHINE_FORM).status_code == 401
        ok = c.post("/builder/forms", json=MACHINE_FORM, headers={"X-Admin-Token": "s3cret"})
        assert ok.status_code == 200
