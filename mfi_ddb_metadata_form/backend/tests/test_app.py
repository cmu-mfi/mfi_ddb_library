import copy
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app import config as config_module
from app import publisher
from app.config import ConfigManager, parse_config, resolve_auto
from app.payload import SubmissionError, build_payload, lint_config
from app import users as users_module

_REPO_CONFIG = Path(__file__).resolve().parents[2] / "config" / "form-config.yaml"
DEFAULT_CONFIG = _REPO_CONFIG if _REPO_CONFIG.exists() else Path("/app/config/form-config.yaml")


@pytest.fixture
def raw_config():
    with open(DEFAULT_CONFIG, encoding="utf-8") as fh:
        return yaml.safe_load(fh)


@pytest.fixture
def cfg(raw_config):
    return parse_config(raw_config)


REGISTERED = [
    {"user_id": "superadmin", "domain": "superadmin", "name": "Super Admin"},
    {"user_id": "jsmith", "domain": "ANDREW", "name": "J Smith"},
    {"user_id": "op", "domain": "ANDREW", "name": "Operator"},
]
ADMIN = {"user_id": "superadmin", "domain": "superadmin"}
OP = {"user_id": "op", "domain": "ANDREW"}


@pytest.fixture(autouse=True)
def fake_users(monkeypatch):
    """No database in the tests: the metadata store holds REGISTERED."""
    monkeypatch.setattr(
        users_module.UserDirectory, "_fetch", staticmethod(lambda db, timeout: list(REGISTERED))
    )
    users_module.directory._users = None  # drop anything cached by an earlier test


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
                {"user": {"user_id": "jsmith", "domain": "ANDREW"}, "role": "admin"},
                {"user": None, "role": ""},  # blank row ignored
            ],
            "created_by": ADMIN,
        },
        cfg,
    )
    assert payload["schema_version"] == "mfi-v1.0"
    assert payload["msg_type"] == "project"
    assert payload["form_id"] == "project"
    assert payload["user_roles"] == [{"user_id": "jsmith", "domain": "ANDREW", "role": "admin"}]
    assert payload["created_by_user_id"] == "superadmin"
    assert payload["created_by_domain"] == "superadmin"
    assert "created_by" not in payload
    assert payload["timestamp"].endswith("Z")


def test_required_and_pattern_errors(cfg):
    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("project"), {"project_id": "has spaces"}, cfg)
    assert exc.value.errors[0]["field"] == "project_id"

    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("user"), {"domain": "ANDREW"}, cfg)
    assert {e["field"] for e in exc.value.errors} == {"user_id", "created_by"}


def test_nested_list_errors_have_paths(cfg):
    with pytest.raises(SubmissionError) as exc:
        build_payload(
            cfg.get_form("project"),
            {"project_id": "p1", "created_by": ADMIN,
             "user_roles": [{"user": {"user_id": "jsmith", "domain": "ANDREW"}, "role": "boss"}]},
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
            "trial_user": OP,
            "created_by": ADMIN,
        },
        cfg,
    )
    assert payload["trial_user_id"] == "op" and payload["trial_user_domain"] == "ANDREW"
    assert payload["time_start"] == "2026-11-05T15:00:00Z"
    assert payload["time_end"] == "2026-11-05T16:30:00Z"


def test_user_field_must_be_a_registered_user(cfg):
    form = cfg.get_form("user")
    payload = build_payload(form, {"user_id": "u", "domain": "D", "created_by": "jsmith@ANDREW"}, cfg)
    assert (payload["created_by_user_id"], payload["created_by_domain"]) == ("jsmith", "ANDREW")

    with pytest.raises(SubmissionError) as exc:
        build_payload(form, {"user_id": "u", "domain": "D",
                             "created_by": {"user_id": "ghost", "domain": "ANDREW"}}, cfg)
    assert exc.value.errors == [
        {"field": "created_by", "message": "Created by: 'ghost' (ANDREW) is not a registered user"}
    ]

    # typing the payload keys directly cannot bypass the dropdown
    with pytest.raises(SubmissionError) as exc:
        build_payload(form, {"user_id": "u", "domain": "D",
                             "created_by_user_id": "x", "created_by_domain": "y"}, cfg)
    assert {e["field"] for e in exc.value.errors} == {"created_by"}


def test_user_field_reports_unreachable_store(cfg):
    def down():
        raise users_module.UserDirectoryError("Cannot load users from the metadata store: down")

    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("trial-tag"), {
            "trial_id": "t", "project_id": "p", "time_start": "2026-01-01T00:00:00Z",
            "time_end": "2026-01-01T01:00:00Z", "trial_user": OP, "created_by": ADMIN,
        }, cfg, users=down)
    assert {e["field"] for e in exc.value.errors} == {"trial_user", "created_by"}
    assert "metadata store" in exc.value.errors[0]["message"]


def test_user_field_has_no_default(raw_config):
    raw_config["forms"][0]["fields"].append({"name": "owner", "type": "user", "default": "superadmin"})
    with pytest.raises(Exception, match="cannot have 'default'"):
        parse_config(raw_config)


def test_lint_requires_a_user_field(raw_config):
    form = raw_config["forms"][1]
    form["fields"] = [f for f in form["fields"] if f["name"] != "created_by"]
    warnings = lint_config(parse_config(raw_config))
    assert any("form 'user'" in w and "type 'user'" in w for w in warnings)


def test_non_editable_auto_cannot_be_spoofed(cfg):
    payload = build_payload(
        cfg.get_form("user"),
        {"user_id": "u", "domain": "D", "timestamp": "1999-01-01", "created_by": ADMIN},
        cfg,
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
                "trial_user": OP,
                "created_by": ADMIN,
            },
            cfg,
        )
    assert "at least one of: project_id, project_name" in exc.value.errors[0]["message"]


def test_custom_data_form(raw_config):
    next(f for f in raw_config["forms"] if f["id"] == "trial-note")["enabled"] = True
    cfg = parse_config(raw_config)
    payload = build_payload(
        cfg.get_form("trial-note"),
        {"trial_id": "t-9", "note": "ok", "temperature_c": "21.5", "passed_qc": True, "created_by": ADMIN},
        cfg,
    )
    assert payload["temperature_c"] == 21.5 and payload["passed_qc"] is True
    with pytest.raises(SubmissionError):
        build_payload(cfg.get_form("trial-note"), {"trial_id": "t", "note": "x", "temperature_c": 500, "created_by": ADMIN}, cfg)


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

    payload = build_payload(cfg.get_form("user"), {"user_id": "u1", "domain": "ANDREW", "created_by": ADMIN}, cfg)
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
    monkeypatch.setenv("FORM_SETTINGS", str(tmp_path / "settings.json"))

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
    assert {e["field"] for e in bad.json()["errors"]} == {"user_id", "created_by"}

    users = client.get("/users").json()
    assert users["ok"] and [u["user_id"] for u in users["users"]] == ["superadmin", "jsmith", "op"]

    ok = client.post("/forms/user/submit",
                     json={"values": {"user_id": "u1", "domain": "ANDREW", "created_by": ADMIN}})
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
        "trial_id", "created_by", "machine", "spindle_rpm", "coolant_on", "operator", "timestamp"
    ]
    assert form["fields"][1]["type"] == "user" and form["fields"][1]["required"] is True

    # The person filling in the form supplies only the values.
    sent = client.post(
        f"/forms/{form_id}/submit",
        json={"values": {"trial_id": "t-1", "machine": "Haas VF-2", "spindle_rpm": "8000",
                         "coolant_on": True, "operator": "jdoe", "created_by": OP}},
    )
    assert sent.status_code == 200, sent.text
    payload = sent.json()["payload"]
    assert payload["msg_type"] == "data"
    assert payload["form_id"] == form_id  # hidden: not a visible field
    assert "form_id" not in [f["name"] for f in form["fields"]]
    assert payload["machine"] == "Haas VF-2" and payload["spindle_rpm"] == 8000
    assert payload["coolant_on"] is True and payload["timestamp"].endswith("Z")
    assert payload["created_by_user_id"] == "op"

    bad = client.post(f"/forms/{form_id}/submit",
                      json={"values": {"trial_id": "t", "machine": "Lathe", "spindle_rpm": "99999",
                                       "created_by": OP}})
    assert {e["field"] for e in bad.json()["errors"]} == {"machine", "spindle_rpm"}

    edited = dict(MACHINE_FORM, keys=MACHINE_FORM["keys"][:1])
    assert client.put(f"/builder/forms/{form_id}", json=edited).status_code == 200
    form = next(f for f in client.get("/config").json()["forms"] if f["id"] == form_id)
    assert [f["name"] for f in form["fields"]] == ["trial_id", "created_by", "machine", "timestamp"]

    assert client.delete(f"/builder/forms/{form_id}").status_code == 200
    assert form_id not in [f["id"] for f in client.get("/config").json()["forms"]]


def test_builder_sections_group_keys_without_data(client):
    form = {
        "title": "Powder sections",
        "keys": [
            {"type": "section", "label": "Material", "help": "What the powder is"},
            {"name": "alloy", "label": "Alloy", "required": True},
            {"type": "section", "label": "Particle size"},
            {"name": "d50", "label": "D50", "type": "number"},
        ],
    }
    created = client.post("/builder/forms", json=form)
    assert created.status_code == 200, created.text
    saved = created.json()["form"]
    assert [k["name"] for k in saved["keys"]] == ["_section_1", "alloy", "_section_2", "d50"]

    view = next(f for f in client.get("/config").json()["forms"] if f["id"] == saved["id"])
    section = next(f for f in view["fields"] if f["name"] == "_section_1")
    assert section["type"] == "section" and section["label"] == "Material"
    assert section["help"] == "What the powder is"

    sent = client.post(f"/forms/{saved['id']}/preview", json={"values": {
        "trial_id": "t", "created_by": ADMIN, "alloy": "316L", "d50": "30",
        "_section_1": "ignored",
    }}).json()["payload"]
    assert sent["alloy"] == "316L" and sent["d50"] == 30
    assert not any(k.startswith("_section") for k in sent)  # headings send no data

    # editing keeps the names; a section needs a title
    assert client.put(f"/builder/forms/{saved['id']}", json=dict(form, keys=saved["keys"])).status_code == 200
    untitled = client.post("/builder/forms", json={"title": "X", "keys": [{"type": "section"}, {"name": "a"}]})
    assert untitled.status_code == 422 and "title" in untitled.text


def test_yaml_section_cannot_be_required(raw_config):
    raw_config["forms"][0]["fields"].insert(0, {"name": "info", "type": "section", "required": True})
    with pytest.raises(Exception, match="only a heading"):
        parse_config(raw_config)


def test_builder_validation_messages(client):
    resp = client.post("/builder/forms", json={
        "title": "Bad", "keys": [
            {"name": "has space"}, {"name": "form_id"}, {"name": "pick", "type": "select"}
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
    monkeypatch.setenv("FORM_SETTINGS", str(tmp_path / "settings.json"))

    import importlib

    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        assert c.get("/config").json()["builder"]["requires_token"] is True
        assert c.post("/builder/forms", json=MACHINE_FORM).status_code == 401
        ok = c.post("/builder/forms", json=MACHINE_FORM, headers={"X-Admin-Token": "s3cret"})
        assert ok.status_code == 200


# --------------------------------------------------------------------------- per-form site


def test_form_site_defaults_and_override(raw_config):
    cfg = parse_config(raw_config)
    project = cfg.get_form("project")
    assert config_module.form_site(cfg, project) == "Mill-19"

    raw_config["forms"][0]["site"] = "SC-Nanofab"
    cfg = parse_config(raw_config)
    project = cfg.get_form("project")
    assert config_module.form_site(cfg, project) == "SC-Nanofab"
    assert publisher.topic_for(cfg, "project", "SC-Nanofab") == "mfi-v1.0-kv/CMU/SC-Nanofab/project/data"
    # the default site is untouched for the other forms
    assert config_module.form_site(cfg, cfg.get_form("user")) == "Mill-19"


def test_form_site_must_be_a_topic_level(raw_config):
    raw_config["forms"][0]["site"] = "bad/site"
    with pytest.raises(Exception, match="must not contain"):
        parse_config(raw_config)


def test_publish_uses_form_site(cfg, monkeypatch):
    import mfi_ddb

    seen = {}

    class FakeStreamer:
        def __init__(self, config, adapter, stream_on_update=False):
            seen["site"] = config["mqtt"]["site"]

        def disconnect(self):
            pass

    monkeypatch.setattr(mfi_ddb, "Streamer", FakeStreamer)
    monkeypatch.setattr(publisher, "check_broker", lambda cfg, timeout=None: None)
    cfg.publish.settle_seconds = 0
    payload = build_payload(cfg.get_form("user"), {"user_id": "u1", "domain": "ANDREW", "created_by": ADMIN}, cfg)

    result = publisher.publish(payload, cfg, "SC-Nanofab")
    assert seen["site"] == "SC-Nanofab"
    assert result["topic"] == "mfi-v1.0-kv/CMU/SC-Nanofab/user/data"
    assert cfg.streamer["mqtt"]["site"] == "Mill-19"  # shared config not mutated


# --------------------------------------------------------------------------- settings page


def test_settings_roundtrip(client):
    view = client.get("/settings").json()
    assert view["overridden"] == []
    assert view["mqtt"]["has_password"] is False
    assert {f["id"] for f in view["forms"]} == {"project", "user", "trial-tag"}

    saved = client.put("/settings", json={
        "mqtt": {"broker_address": "broker.example", "broker_port": 1884,
                 "enterprise": "CMU-Test", "username": "bob", "password": "s3cret"},
        "form_sites": {"project": "SC-Nanofab"},
    })
    assert saved.status_code == 200, saved.text
    body = saved.json()
    assert body["mqtt"]["broker_address"] == "broker.example"
    assert body["mqtt"]["has_password"] is True
    assert "s3cret" not in saved.text  # password is write-only

    config_view = client.get("/config").json()
    forms = {f["id"]: f for f in config_view["forms"]}
    assert forms["project"]["topic_prefix"] == "mfi-v1.0-kv/CMU-Test/SC-Nanofab"
    assert forms["user"]["topic_prefix"] == "mfi-v1.0-kv/CMU-Test/Mill-19"
    assert config_view["target"]["broker"] == "broker.example:1884"
    assert "s3cret" not in str(config_view)

    # preview / submit use the form's site
    preview = client.post("/forms/project/preview",
                          json={"values": {"project_id": "p1", "created_by": ADMIN}}).json()
    assert preview["topic"] == "mfi-v1.0-kv/CMU-Test/SC-Nanofab/project/data"

    # omitting the password keeps it; clearing a site falls back to the default
    again = client.put("/settings", json={"mqtt": {"broker_port": 1885}, "form_sites": {"project": ""}}).json()
    assert again["mqtt"]["has_password"] is True and again["mqtt"]["broker_port"] == 1885
    assert {f["id"]: f["site"] for f in again["forms"]}["project"] == "Mill-19"

    reset = client.delete("/settings").json()
    assert reset["overridden"] == [] and reset["mqtt"]["has_password"] is False


def test_settings_validation(client):
    bad = client.put("/settings", json={"mqtt": {"enterprise": "a/b", "broker_port": 70000}})
    assert bad.status_code == 422
    assert client.put("/settings", json={"form_sites": {"user": "x#y"}}).status_code == 422
    assert client.put("/settings", json={"mqtt": {"nope": 1}}).status_code == 422


def test_settings_test_connection_reports_failure(client):
    res = client.post("/settings/test", json={"mqtt": {"broker_address": "127.0.0.1", "broker_port": 1}})
    assert res.status_code == 200
    assert res.json()["ok"] is False and res.json()["target"] == "127.0.0.1:1"


def test_builder_form_site(client):
    form = dict(MACHINE_FORM, site="SC-Nanofab")
    created = client.post("/builder/forms", json=form).json()
    assert created["form"]["site"] == "SC-Nanofab"
    view = {f["id"]: f for f in client.get("/config").json()["forms"]}
    assert view[created["form"]["id"]]["topic_prefix"] == "mfi-v1.0-kv/CMU/SC-Nanofab"
    assert client.post("/builder/forms", json=dict(MACHINE_FORM, site="a/b")).status_code == 422


# --------------------------------------------------------------------------- session user


def test_session_user_matches_created_by(cfg, monkeypatch):
    import mfi_ddb

    seen = {}

    class FakeStreamer:
        def __init__(self, config, adapter, stream_on_update=False):
            seen["user"] = config["user"]

        def disconnect(self):
            pass

    monkeypatch.setattr(mfi_ddb, "Streamer", FakeStreamer)
    monkeypatch.setattr(publisher, "check_broker", lambda cfg, timeout=None: None)
    cfg.publish.settle_seconds = 0

    values = {"user_id": "u1", "domain": "ANDREW", "created_by": ADMIN}
    payload = build_payload(cfg.get_form("user"), values, cfg)
    publisher.publish(payload, cfg)
    assert seen["user"]["user_id"] == "superadmin" and seen["user"]["domain"] == "superadmin"
    assert cfg.streamer["user"]["user_id"] == "metadata-form"  # shared config not mutated

    # no created_by in the payload -> the app identity
    assert publisher.session_user({"msg_type": "data"}, cfg)["user_id"] == "metadata-form"


# --------------------------------------------------------------------------- entries (RWS type1)

POWDER_FORM = {
    "title": "Powder Library",
    "keys": [
        {"name": "alloy", "type": "text", "required": True},
        {"name": "vendor", "type": "select", "required": True, "options": ["EOS", "6K", "Ametek"]},
        {"name": "date_received", "type": "date"},
        {"name": "d50", "type": "number"},
    ],
}


def _trial(name, payload, birth="2026-11-05T19:21:41+00:00"):
    """A type1 trial shaped like the metadata store returns for a form submission."""
    return {
        "uuid": f"uuid-{name}",
        "trial_name": name,
        "birth_timestamp": birth,
        "metadata": {"adapter": {"config": {"payloads": [payload]}}},
    }


def _powder(trial_id, alloy, vendor, d50, received, ts, user="dsinclai", form_id="powder-library"):
    return {
        "schema_version": "mfi-v1.0", "msg_type": "data", "form_id": form_id, "trial_id": trial_id,
        "created_by_user_id": user, "created_by_domain": "andrew", "alloy": alloy, "vendor": vendor,
        "d50": d50, "date_received": received, "timestamp": ts,
    }


@pytest.fixture
def rws(monkeypatch):
    """Fake RWS type1: records the request and returns the trials in `rws.trials`."""
    from app import entries

    class Fake:
        trials: list = []
        requests: list = []

    def fake_call(cfg, body):
        Fake.requests.append(body)
        return Fake.trials

    monkeypatch.setattr(entries, "_call_type1", fake_call)
    Fake.trials = [
        _trial("a", _powder("a", "17-4 PH", "EOS", 35.0, "2017-02-01", "2026-11-05T19:21:40Z")),
        _trial("b", _powder("b", "Ti-6Al-4V", "6K", 42.5, "2024-06-10", "2026-11-04T10:00:00Z", user="sbirchal")),
        _trial("c", _powder("c", "316L", "Ametek", 20.0, "2025-01-15", "2026-11-03T08:00:00Z")),
        # another form whose id merely contains the search term
        _trial("d", _powder("d", "17-4 PH", "EOS", 30.0, "2017-02-01", "2026-11-05T09:00:00Z",
                            form_id="powder-library-2")),
    ]
    Fake.requests = []
    return Fake


def test_entries_search_and_filters(client, rws):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    assert form_id == "powder-library"

    everything = client.post(f"/forms/{form_id}/entries", json={}).json()
    assert everything["ok"] and everything["show_all"] is True
    # newest first; the powder-library-2 entry is dropped even though RWS returned it
    assert [e["trial_id"] for e in everything["entries"]] == ["a", "b", "c"]
    assert everything["entries"][1]["created_by"] == {"user_id": "sbirchal", "domain": "andrew"}
    sent = rws.requests[-1]
    assert sent["search_terms"] == ["powder-library"]
    assert (sent["user_id"], sent["user_domain"]) == ("superadmin", "superadmin")  # show_all

    def ids(**query):
        res = client.post(f"/forms/{form_id}/entries", json=query)
        assert res.status_code == 200, res.text
        return [e["trial_id"] for e in res.json()["entries"]]

    assert ids(filters={"alloy": {"contains": "17-4"}}) == ["a"]
    assert ids(filters={"vendor": {"any_of": ["6K", "Ametek"]}}) == ["b", "c"]
    assert ids(filters={"d50": {"min": 30, "max": 40}}) == ["a"]
    assert ids(filters={"date_received": {"date_from": "2024-01-01"}}) == ["b", "c"]
    assert ids(entered_from="2026-11-04T00:00:00Z", entered_to="2026-11-04T23:59:59Z") == ["b"]

    ids(text="gas atomization")
    assert rws.requests[-1]["search_terms"] == ["powder-library", "gas", "atomization"]


def test_entries_rejects_bad_filters(client, rws):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    bad = client.post(f"/forms/{form_id}/entries", json={"filters": {"created_by": {"contains": "x"}}})
    assert bad.status_code == 422
    assert client.post("/forms/nope/entries", json={}).status_code == 404


def test_entries_only_own_when_show_all_is_off(tmp_path, raw_config, monkeypatch, rws):
    raw_config["entries"] = {"show_all": False}
    path = tmp_path / "form-config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    for var, name in [("FORM_CONFIG_PATH", "form-config.yaml"), ("FORM_HISTORY_DB", "history.db"),
                      ("FORM_CUSTOM_FORMS", "custom_forms.json"), ("FORM_SETTINGS", "settings.json")]:
        monkeypatch.setenv(var, str(tmp_path / name))

    import importlib

    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        assert c.get("/config").json()["entries"]["show_all"] is False
        assert c.post("/forms/user/entries", json={}).status_code == 422  # who is asking?
        ok = c.post("/forms/user/entries", json={"viewer": {"user_id": "dsinclai", "domain": "andrew"}})
        assert ok.status_code == 200
        assert (rws.requests[-1]["user_id"], rws.requests[-1]["user_domain"]) == ("dsinclai", "andrew")


def test_new_entry_and_update_version_keys(client):
    values = {"user_id": "u1", "domain": "ANDREW", "created_by": ADMIN}
    first = client.post("/forms/user/preview", json={"values": values}).json()["payload"]
    assert first["entry_version"] == 1 and "supersedes" not in first
    assert len(first["entry_id"]) == 36  # uuid4

    update = client.post("/forms/user/preview", json={
        "values": values,
        "revises": {"entry_id": first["entry_id"], "version": 1, "trial_uuid": "trial-uuid-1"},
    }).json()["payload"]
    assert update["entry_id"] == first["entry_id"]
    assert update["entry_version"] == 2 and update["supersedes"] == "trial-uuid-1"

    for bad in ({"entry_id": "e", "version": 0, "trial_uuid": "t"}, {"entry_id": "e"},
                {"entry_id": "a b", "version": 1, "trial_uuid": "t"}):
        res = client.post("/forms/user/preview", json={"values": values, "revises": bad})
        assert res.status_code == 422, bad
        assert res.json()["message"] == "Invalid entry to update."

    taken = client.post("/builder/forms", json={"title": "X", "keys": [{"name": "entry_version"}]})
    assert taken.status_code == 422


def test_entries_show_latest_version_with_history(client, rws):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    v1 = _powder("p1", "17-4 PH", "EOS", 35.0, "2017-02-01", "2026-11-05T10:00:00Z")
    v1.update(entry_id="e-1", entry_version=1)
    v2 = _powder("p1", "17-4 PH", "EOS", 37.5, "2017-02-01", "2026-11-05T12:00:00Z", user="sbirchal")
    v2.update(entry_id="e-1", entry_version=2, supersedes="uuid-p1-v1")
    legacy = _powder("old", "316L", "Ametek", 20.0, "2025-01-15", "2026-11-03T08:00:00Z")
    rws.trials = [_trial("p1-v1", v1), _trial("p1-v2", v2), _trial("old", legacy)]

    body = client.post(f"/forms/{form_id}/entries", json={}).json()
    assert body["count"] == 2
    latest, old = body["entries"]
    assert latest["entry_id"] == "e-1" and latest["version"] == 2
    assert latest["values"]["d50"] == 37.5 and latest["uuid"] == "uuid-p1-v2"
    assert [v["version"] for v in latest["versions"]] == [1]
    assert latest["versions"][0]["values"]["d50"] == 35.0
    # saved before versioning: version 1 of an entry identified by its trial uuid
    assert old["entry_id"] == "uuid-old" and old["version"] == 1 and old["versions"] == []

    # filters look at the current version: d50 35 only matched v1 -> entry not shown
    only_v1 = client.post(f"/forms/{form_id}/entries", json={"filters": {"d50": {"max": 36}}}).json()
    assert [e["trial_id"] for e in only_v1["entries"]] == ["old"]


# --------------------------------------------------------------------------- split by weight

SPLIT_FORM = {
    "title": "Powder Library",  # -> id powder-library, which config/splits maps to 'weight'
    "keys": [
        {"name": "alloy", "type": "text", "required": True},
        {"name": "weight", "type": "number"},
    ],
}
ORIGINAL = {"trial_id": "p1", "alloy": "17-4 PH", "weight": 10, "created_by": None}
OPENED_AT = {"entry_id": "e-1", "version": 2, "trial_uuid": "uuid-p1-v2"}


@pytest.fixture
def split_client(client, rws, monkeypatch):
    """Powder form with a weight, RWS holding entry e-1 at version 2, publishes recorded."""
    import app.main as main

    assert client.post("/builder/forms", json=SPLIT_FORM).json()["form"]["id"] == "powder-library"
    stored = []
    for version in (1, 2):
        p = _powder("p1", "17-4 PH", "EOS", 35.0, "2017-02-01", f"2026-11-05T1{version}:00:00Z")
        p.update(entry_id="e-1", entry_version=version, weight=10)
        stored.append(_trial(f"p1-v{version}", p))
    rws.trials = stored

    published = []

    def fake_publish(payload, cfg, site=None):
        if getattr(fake_publish, "fail_on", None) == len(published):
            from app.publisher import PublishError
            raise PublishError("broker down")
        published.append(payload)
        return {"status": "published", "topic": "t", "trial_id": payload["trial_id"]}

    monkeypatch.setattr(main, "publish", fake_publish)
    client.published = published
    client.fake_publish = fake_publish
    return client


def _split(client, weight, endpoint="submit", original=None, opened_at=None):
    return client.post(f"/forms/powder-library/{endpoint}", json={
        "values": {"trial_id": "p1", "alloy": "17-4 PH", "weight": weight, "created_by": OP},
        "split": {"revises": opened_at or OPENED_AT, "values": original or ORIGINAL},
    })


def test_split_creates_new_entry_and_reduces_original(split_client, rws):
    res = _split(split_client, 4)
    assert res.status_code == 200, res.text
    body = res.json()
    child, parent = split_client.published  # new entry first, then the original
    assert body["payload"] == child and body["parent_payload"] == parent and body["remaining"] == 6

    assert child["weight"] == 4 and child["trial_id"] == "p1"
    assert child["entry_version"] == 1 and child["entry_id"] != "e-1"
    assert child["split_from"] == "e-1"

    assert parent["entry_id"] == "e-1" and parent["entry_version"] == 3
    assert parent["supersedes"] == "uuid-p1-v2"
    assert parent["weight"] == 6 and parent["split_into"] == child["entry_id"]
    assert parent["alloy"] == "17-4 PH"
    assert (parent["created_by_user_id"], parent["created_by_domain"]) == ("op", "ANDREW")
    # stale check looked the original up by its entry id
    assert rws.requests[-1]["search_terms"] == ["powder-library", "e-1"]


def test_split_whole_weight_and_preview(split_client):
    preview = _split(split_client, 10, endpoint="preview").json()
    assert preview["ok"] and preview["remaining"] == 0
    assert preview["parent_payload"]["weight"] == 0 and preview["payload"]["weight"] == 10
    assert split_client.published == []  # preview publishes nothing

    assert _split(split_client, 2.5, endpoint="preview").json()["parent_payload"]["weight"] == 7.5


def test_split_weight_rules(split_client):
    for weight, message in [(12, "more than the original's 10"), (0, "more than 0"), ("", "more than 0")]:
        res = _split(split_client, weight)
        assert res.status_code == 422, weight
        assert res.json()["errors"][0]["field"] == "weight"
        assert message in res.json()["errors"][0]["message"]
    no_weight = _split(split_client, 4, original={**ORIGINAL, "weight": None})
    assert no_weight.status_code == 422 and "no weight" in no_weight.json()["message"]
    assert split_client.published == []


def test_split_refused_when_original_changed(split_client):
    res = _split(split_client, 4, opened_at={**OPENED_AT, "version": 1, "trial_uuid": "uuid-p1-v1"})
    assert res.status_code == 409 and "now version 2" in res.json()["message"]
    assert split_client.published == []


def test_split_original_not_reduced_reports_partial(split_client):
    split_client.fake_publish.fail_on = 1  # the new entry goes out, the original's update fails
    res = _split(split_client, 4)
    assert res.status_code == 502
    body = res.json()
    assert body["split_partial"] is True and "NOT reduced" in body["message"]
    assert len(split_client.published) == 1 and split_client.published[0]["weight"] == 4
    assert body["retry"]["revises"] == OPENED_AT and body["retry"]["values"]["weight"] == 6

    # the retry is a plain update of the original
    split_client.fake_publish.fail_on = None
    again = split_client.post("/forms/powder-library/submit", json=body["retry"])
    assert again.status_code == 200 and again.json()["payload"]["weight"] == 6


def test_split_only_on_configured_forms(split_client):
    res = split_client.post("/forms/user/submit", json={
        "values": {"user_id": "u1", "domain": "ANDREW", "created_by": ADMIN},
        "split": {"revises": OPENED_AT, "values": {}},
    })
    assert res.status_code == 422 and "can't be split" in res.json()["message"]
    assert split_client.post("/builder/forms", json={"title": "X", "keys": [{"name": "split_from"}]}).status_code == 422
    assert split_client.get("/config").json()["splits"] == {"powder-library": "weight"}


# --------------------------------------------------------------------------- batch add


def _batch(client, form_id, content, fmt="csv", created_by=ADMIN):
    body = {"format": fmt, "content": content}
    if created_by is not None:
        body["created_by"] = created_by
    res = client.post(f"/forms/{form_id}/batch/check", json=body)
    assert res.status_code == 200, res.text
    return res.json()


def test_batch_csv_all_rows_ok(client):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    assert client.get("/config").json()["batch"] == {"enabled": True, "max_rows": 200}
    # key names and labels both work as column titles; blank rows are skipped
    csv_text = (
        "﻿trial_id,Alloy,VENDOR,date_received,D50\n"
        "p-1,17-4 PH,EOS,2017-02-01,35.5\n"
        ",,,,\n"
        "p-2,316L,6K,2024-06-10,\n"
    )
    result = _batch(client, form_id, csv_text)
    assert result["ok"] is True, result
    assert result["count"] == 2 and result["errors"] == []
    assert [c["field"] for c in result["columns"]] == ["trial_id", "alloy", "vendor", "date_received", "d50"]
    first, second = result["rows"]
    assert first["row"] == 2 and second["row"] == 4  # spreadsheet row numbers
    assert first["values"]["created_by"] == ADMIN and first["values"]["d50"] == "35.5"

    # the checked values publish unchanged through the normal submit
    sent = client.post(f"/forms/{form_id}/submit", json={"values": first["values"]}).json()
    assert sent["payload"]["d50"] == 35.5 and sent["payload"]["created_by_user_id"] == "superadmin"


def test_batch_reports_every_problem(client):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    result = _batch(client, form_id, (
        "trial_id,alloy,vendor,d50\n"
        "p-1,,EOS,30\n"          # blank required value
        "p-2,316L,Lathe,30\n"    # not one of the choices
        "p-3,316L,EOS,abc\n"     # not a number
        "p-4,316L,EOS,20\n"      # fine
    ))
    assert result["ok"] is False and result["bad_rows"] == 3
    by_row = {r["row"]: {e["field"] for e in r["errors"]} for r in result["rows"]}
    assert by_row == {2: {"alloy"}, 3: {"vendor"}, 4: {"d50"}, 5: set()}


def test_batch_column_problems(client):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    result = _batch(client, form_id, "trial_id,alloy,colour,timestamp,created_by\np-1,316L,red,x,y\n")
    messages = " | ".join(e["message"] for e in result["errors"])
    assert "Missing required column: vendor (vendor)" in messages
    assert "'colour' doesn't match any field" in messages
    assert "'timestamp' is filled in automatically" in messages
    assert "'created_by' is chosen once for the whole batch" in messages
    assert result["ok"] is False

    dup = _batch(client, form_id, "trial_id,alloy,Alloy,vendor\np,a,b,EOS\n")
    assert any("both fill" in e["message"] for e in dup["errors"])


def test_batch_needs_a_registered_created_by(client):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    table = "trial_id,alloy,vendor\np-1,316L,EOS\n"
    missing = _batch(client, form_id, table, created_by=None)
    assert any("Created by" in e["message"] for e in missing["errors"]) and not missing["ok"]
    ghost = _batch(client, form_id, table, created_by={"user_id": "ghost", "domain": "x"})
    assert any("not a registered user" in e["message"] for e in ghost["errors"]) and not ghost["ok"]


def test_batch_pasted_cells_and_excel_match_csv(client):
    import base64
    import io
    from datetime import date

    from openpyxl import Workbook

    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    pasted = _batch(client, form_id, "trial_id\talloy\tvendor\td50\np-1\t17-4 PH\tEOS\t35\n", fmt="tsv")
    assert pasted["ok"] and pasted["rows"][0]["values"]["alloy"] == "17-4 PH"

    book = Workbook()
    sheet = book.active
    sheet.append(["Trial ID", "Alloy", "Vendor", "Date Received", "D50"])
    sheet.append(["p-1", "17-4 PH", "EOS", date(2017, 2, 1), 35.0])
    buf = io.BytesIO()
    book.save(buf)
    xlsx = _batch(client, form_id, base64.b64encode(buf.getvalue()).decode(), fmt="xlsx")
    assert xlsx["ok"], xlsx
    values = xlsx["rows"][0]["values"]
    assert values["date_received"] == "2017-02-01" and values["d50"] == 35

    not_excel = client.post(f"/forms/{form_id}/batch/check",
                            json={"format": "xlsx", "content": base64.b64encode(b"hello").decode(),
                                  "created_by": ADMIN})
    assert not_excel.status_code == 422


def test_batch_row_limit(client):
    form_id = client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    table = "trial_id,alloy,vendor\n" + "".join(f"p-{i},316L,EOS\n" for i in range(201))
    result = _batch(client, form_id, table)
    assert not result["ok"] and any("at most 200" in e["message"] for e in result["errors"])


# --------------------------------------------------------------------------- labels

from app.labels import LabelEntry, label_record, owner_display, short_entry_id  # noqa: E402

LABEL_TEMPLATE = """<html><body><div class="card"><div class="card-title">{{ file_id }}</div>
<p>Alloy: {{ alloy }}</p><p>Owner: {{ owner_displ }}</p><p>Size: {{ D10 }} - {{ D90 }} um</p>
<p>Labeled: {{ label_tools.now(fmt="%Y-%m-%d") }}</p></div></body></html>"""


def test_label_record_mapping_and_escaping():
    assert short_entry_id("e30717d6-722d-4dc0-b79e-eab2f45ec2e0") == "e30717d6722d"
    assert owner_display("dsinclai", "Daniel Sinclair") == "D. Sinclair"
    assert owner_display("dsinclai", "") == "dsinclai"
    mapping = {"alloy": "alloy", "production": "production_method", "D50": "d50"}
    entry = LabelEntry(entry_id="e30717d6-722d-4dc0", version=2, trial_id="test",
                       values={"alloy": "<b>17-4</b>", "d10": 24.0, "created_by_user_id": "jsmith",
                               "created_by_domain": "ANDREW"})
    record = label_record(mapping, entry, {("jsmith", "ANDREW"): {"name": "J Smith"}})
    assert record["file_id"] == "e30717d6722d"
    assert record["alloy"] == "&lt;b&gt;17-4&lt;/b&gt;"  # rendered literally, not as markup
    assert record["production"] == "" and record["D50"] == ""  # missing -> empty
    assert record["owner_displ"] == "J. Smith"
    assert record["trial_id"] == "test" and record["entry_version"] == "2"


@pytest.fixture
def label_client(tmp_path, raw_config, monkeypatch):
    templates = tmp_path / "labels"
    templates.mkdir()
    for size in ("S", "L"):
        (templates / f"label_template_{size}.html").write_text(LABEL_TEMPLATE)
        # like label_maker_main's styles: a flex body that would put labels side by side
        (templates / f"style{size}.css").write_text("body { display: flex; min-height: 100vh; font-size: 9pt }")
    raw_config["publish"]["dry_run"] = True
    raw_config["labels"]["template_dir"] = str(templates)
    path = tmp_path / "form-config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    for var, name in [("FORM_CONFIG_PATH", "form-config.yaml"), ("FORM_HISTORY_DB", "history.db"),
                      ("FORM_CUSTOM_FORMS", "custom_forms.json"), ("FORM_SETTINGS", "settings.json")]:
        monkeypatch.setenv(var, str(tmp_path / name))

    import importlib

    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c


def test_print_labels_pdf(label_client):
    form_id = label_client.post("/builder/forms", json=POWDER_FORM).json()["form"]["id"]
    view = label_client.get("/config").json()["labels"]
    assert sorted(view["sizes"]) == ["L", "S"] and view["forms"] == ["powder-library"]
    assert label_client.get("/health").json()["labels_error"] is None

    entries = [
        {"entry_id": "e30717d6-722d-4dc0-b79e-eab2f45ec2e0", "version": 2, "trial_id": "test",
         "values": _powder("test", "17-4 PH", "EOS", 35.0, "2017-02-01", "2026-11-05T19:21:40Z")},
        {"entry_id": "0f0e0d0c-0b0a-4908-8706-050403020100", "version": 1, "trial_id": "test3",
         "values": _powder("test3", "Ti-6Al-4V", "6K", 42.5, "2024-06-10", "2026-11-05T20:27:42Z")},
    ]
    res = label_client.post("/labels", json={"form_id": form_id, "size": "S", "entries": entries})
    assert res.status_code == 200, res.text
    assert res.headers["content-type"] == "application/pdf"
    assert res.content.startswith(b"%PDF")
    import io

    from pypdf import PdfReader

    pdf = PdfReader(io.BytesIO(res.content))
    assert len(pdf.pages) == 2  # one label per page
    box = pdf.pages[0].mediabox
    assert (round(float(box.width) / 72, 2), round(float(box.height) / 72, 2)) == (2.31, 4.0)  # S label stock
    first = pdf.pages[0].extract_text()
    assert "e30717d6722d" in first and "17-4 PH" in first  # short entry ID title + values

    bad_size = label_client.post("/labels", json={"form_id": form_id, "size": "XL", "entries": entries})
    assert bad_size.status_code == 422
    no_label = label_client.post("/labels", json={"form_id": "user", "size": "S", "entries": entries})
    assert no_label.status_code == 404
    empty = label_client.post("/labels", json={"form_id": form_id, "size": "S", "entries": []})
    assert empty.status_code == 422


def test_labels_hidden_when_templates_missing(client):
    # the default test config points at /app/labels, which doesn't exist in the test image
    assert client.get("/config").json()["labels"] == {"sizes": [], "forms": []}
    assert "not found" in client.get("/health").json()["labels_error"]


def test_entries_reports_unreachable_rws(client, raw_config):
    res = client.post("/forms/user/entries", json={})
    # the test config points at localhost:8000 where nothing listens
    assert res.status_code in (502, 503) and "retrieval API" in res.json()["message"]
