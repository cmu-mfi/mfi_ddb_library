import copy
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

from app import config as config_module
from app import publisher
from app.config import ConfigManager, parse_config, resolve_auto
from app.payload import SubmissionError, build_payload, lint_config

# Sample "Created by" for test submissions. The field is required and has no
# pre-fill, so every sample names someone; tests don't look it up (the user
# directory is off), so this is just a neutral placeholder.
CREATOR = {"created_by_user_id": "test_creator", "created_by_domain": "TEST"}

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
            **CREATOR,
        },
        cfg,
    )
    assert payload["schema_version"] == "mfi-v1.0"
    assert payload["msg_type"] == "project"
    assert payload["user_roles"] == [{"user_id": "jsmith", "domain": "ANDREW", "role": "admin"}]
    assert payload["created_by_user_id"] == "test_creator"
    assert payload["timestamp"].endswith("Z")


def test_required_and_pattern_errors(cfg):
    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("project"), {"project_id": "has spaces"}, cfg)
    assert exc.value.errors[0]["field"] == "project_id"

    with pytest.raises(SubmissionError) as exc:
        build_payload(cfg.get_form("user"), {"domain": "ANDREW"}, cfg)
    # Created by has no pre-fill, so it is required like the user ID
    assert {e["field"] for e in exc.value.errors} == {"user_id", "created_by_user_id"}


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
            **CREATOR,
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
        cfg.get_form("user"), {"user_id": "u", "domain": "D", "timestamp": "1999-01-01", **CREATOR}, cfg
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
                **CREATOR,
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
    assert publisher.topic_for(cfg, "project") == "mfi-v1.0-kv/CMU/M19/project/data"


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

    payload = build_payload(cfg.get_form("user"), {"user_id": "u1", "domain": "ANDREW", **CREATOR}, cfg)
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
    assert {e["field"] for e in bad.json()["errors"]} == {"user_id", "created_by_user_id"}

    ok = client.post("/forms/user/submit", json={"values": {"user_id": "u1", "domain": "ANDREW", **CREATOR}})
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
        "trial_id", "machine", "spindle_rpm", "coolant_on", "operator",
        "created_by_user_id", "created_by_domain", "timestamp",
    ]
    created_by = next(f for f in form["fields"] if f["name"] == "created_by_user_id")
    assert created_by["required"] and created_by["lookup"] == "user"
    assert created_by["domain_field"] == "created_by_domain"

    # The person filling in the form supplies only the values.
    sent = client.post(
        f"/forms/{form_id}/submit",
        json={"values": {"trial_id": "t-1", "machine": "Haas VF-2", "spindle_rpm": "8000",
                         "coolant_on": True, "operator": "jdoe", **CREATOR}},
    )
    assert sent.status_code == 200, sent.text
    payload = sent.json()["payload"]
    assert payload["msg_type"] == "data"
    assert payload["machine"] == "Haas VF-2" and payload["spindle_rpm"] == 8000
    assert payload["coolant_on"] is True and payload["timestamp"].endswith("Z")
    assert (payload["created_by_user_id"], payload["created_by_domain"]) == ("test_creator", "TEST")

    # Created by is required: there is no pre-filled creator any more
    missing = client.post(f"/forms/{form_id}/preview",
                          json={"values": {"trial_id": "t-1", "machine": "Haas VF-2"}})
    assert missing.status_code == 422
    assert "created_by_user_id" in {e["field"] for e in missing.json()["errors"]}

    bad = client.post(f"/forms/{form_id}/submit",
                      json={"values": {"trial_id": "t", "machine": "Lathe", "spindle_rpm": "99999", **CREATOR}})
    assert {e["field"] for e in bad.json()["errors"]} == {"machine", "spindle_rpm"}

    edited = dict(MACHINE_FORM, keys=MACHINE_FORM["keys"][:1])
    assert client.put(f"/builder/forms/{form_id}", json=edited).status_code == 200
    form = next(f for f in client.get("/config").json()["forms"] if f["id"] == form_id)
    assert [f["name"] for f in form["fields"]] == [
        "trial_id", "machine", "created_by_user_id", "created_by_domain", "timestamp"
    ]

    # the automatic fields can't be redefined as keys
    clash = dict(MACHINE_FORM, keys=[{"name": "created_by_user_id"}])
    res = client.put(f"/builder/forms/{form_id}", json=clash)
    assert res.status_code == 422 and "added automatically" in res.text

    # a builder form with superadmin as creator is refused like any other form
    res = client.post(f"/forms/{form_id}/preview", json={"values": {
        "trial_id": "t-1", "machine": "Haas VF-2",
        "created_by_user_id": "superadmin", "created_by_domain": "superadmin"}})
    assert res.status_code == 403 and res.json()["errors"][0]["field"] == "created_by_user_id"

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
    assert config_module.form_site(cfg, project) == "M19"

    raw_config["forms"][0]["site"] = "SC-Nanofab"
    cfg = parse_config(raw_config)
    project = cfg.get_form("project")
    assert config_module.form_site(cfg, project) == "SC-Nanofab"
    assert publisher.topic_for(cfg, "project", "SC-Nanofab") == "mfi-v1.0-kv/CMU/SC-Nanofab/project/data"
    # the default site is untouched for the other forms
    assert config_module.form_site(cfg, cfg.get_form("user")) == "M19"


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
    payload = build_payload(cfg.get_form("user"), {"user_id": "u1", "domain": "ANDREW", **CREATOR}, cfg)

    result = publisher.publish(payload, cfg, "SC-Nanofab")
    assert seen["site"] == "SC-Nanofab"
    assert result["topic"] == "mfi-v1.0-kv/CMU/SC-Nanofab/user/data"
    assert cfg.streamer["mqtt"]["site"] == "M19"  # shared config not mutated


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
    assert forms["user"]["topic_prefix"] == "mfi-v1.0-kv/CMU-Test/M19"
    assert config_view["target"]["broker"] == "broker.example:1884"
    assert "s3cret" not in str(config_view)

    # preview / submit use the form's site
    preview = client.post("/forms/project/preview", json={"values": {"project_id": "p1", **CREATOR}}).json()
    assert preview["topic"] == "mfi-v1.0-kv/CMU-Test/SC-Nanofab/project/data"

    # omitting the password keeps it; clearing a site falls back to the default
    again = client.put("/settings", json={"mqtt": {"broker_port": 1885}, "form_sites": {"project": ""}}).json()
    assert again["mqtt"]["has_password"] is True and again["mqtt"]["broker_port"] == 1885
    assert {f["id"]: f["site"] for f in again["forms"]}["project"] == "M19"

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

    values = {"user_id": "u1", "domain": "ANDREW",
              "created_by_user_id": "superadmin", "created_by_domain": "superadmin"}
    payload = build_payload(cfg.get_form("user"), values, cfg)
    publisher.publish(payload, cfg)
    assert seen["user"]["user_id"] == "superadmin" and seen["user"]["domain"] == "superadmin"
    assert cfg.streamer["user"]["user_id"] == "metadata-form"  # shared config not mutated

    # no created_by in the payload -> the app identity
    assert publisher.session_user({"msg_type": "data"}, cfg)["user_id"] == "metadata-form"


# --------------------------------------------------------------------------- user directory

REGISTERED = {("superadmin", "superadmin"), ("test_member", "ANDREW")}


def test_lookup_fields_in_default_config(cfg):
    project = cfg.get_form("project")
    created_by = next(f for f in project.fields if f.name == "created_by_user_id")
    assert created_by.lookup == "user" and created_by.domain_field == "created_by_domain"
    member = next(f for f in project.fields if f.name == "user_roles").fields[0]
    assert member.lookup == "user" and member.domain_field == "domain"


def test_domain_field_must_exist(raw_config):
    created_by = next(f for f in raw_config["forms"][0]["fields"] if f["name"] == "created_by_user_id")
    created_by["domain_field"] = "nope"
    with pytest.raises(Exception, match="no such field"):
        parse_config(raw_config)


def test_domain_field_needs_lookup(raw_config):
    raw_config["forms"][0]["fields"][0]["domain_field"] = "description"
    with pytest.raises(Exception, match="only applies with 'lookup: user'"):
        parse_config(raw_config)


def test_user_refs_include_list_members(cfg):
    from app import directory

    payload = {
        "project_id": "p1",
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "user_roles": [{"user_id": "jdoe", "domain": "ANDREW", "role": "admin"}, {"user_id": "", "domain": ""}],
    }
    refs = directory.user_refs(cfg.get_form("project"), payload)
    assert refs == [
        ("user_roles.0.user_id", "jdoe", "ANDREW"),
        ("created_by_user_id", "superadmin", "superadmin"),
    ]


def test_check_users_warns_for_unregistered(raw_config, monkeypatch):
    from app import directory

    raw_config["directory"]["rws_url"] = "http://rws.test:8000"
    cfg = parse_config(raw_config)
    monkeypatch.setattr(
        directory, "get_user", lambda cfg, u, d: {"user_id": u, "domain": d} if (u, d) in REGISTERED else None
    )
    payload = {"created_by_user_id": "ghost", "created_by_domain": "ANDREW",
               "user_roles": [{"user_id": "test_member", "domain": "ANDREW"}]}
    warnings = directory.check_users(cfg.get_form("project"), payload, cfg)
    assert [w["field"] for w in warnings] == ["created_by_user_id"]
    assert "ghost / ANDREW is not a registered user" in warnings[0]["message"]


def test_check_users_off_or_unreachable(raw_config, monkeypatch):
    from app import directory

    payload = {"created_by_user_id": "ghost", "created_by_domain": "ANDREW"}
    raw_config["directory"]["rws_url"] = ""
    assert directory.check_users(parse_config(raw_config).get_form("project"), payload, parse_config(raw_config)) == []

    raw_config["directory"]["rws_url"] = "http://127.0.0.1:1"  # nothing listens here
    raw_config["directory"]["timeout_seconds"] = 2
    cfg = parse_config(raw_config)
    errors = directory.check_users(cfg.get_form("project"), payload, cfg)
    # can't confirm registration -> refused, with a general (not per-field) message
    assert len(errors) == 1 and errors[0]["field"] is None
    assert "Could not confirm that the people entered are registered" in errors[0]["message"]


def test_directory_endpoints(client, monkeypatch):
    import app.main as main

    # not configured in the test config -> available: false, form falls back to text
    assert client.get("/directory/users", params={"search": "s"}).json()["available"] is False
    assert client.get("/config").json()["directory"] == {"enabled": False}

    monkeypatch.setattr(main.config_manager.get().directory, "rws_url", "http://rws.test:8000")
    monkeypatch.setattr(main, "directory_search_users", lambda cfg, s, n: [{"user_id": "test_member", "domain": "ANDREW", "name": "S"}])
    monkeypatch.setattr(main, "directory_get_user", lambda cfg, u, d: None)
    found = client.get("/directory/users", params={"search": "ster"}).json()
    assert found["available"] is True and found["users"][0]["user_id"] == "test_member"
    missing = client.get("/directory/users/ANDREW/ghost").json()
    assert missing == {"available": True, "registered": False, "user": None, "reserved": False}


# --------------------------------------------------------------------------- reserved accounts

SUPERADMIN_CREATOR = {"user_id": "u1", "domain": "ANDREW",
                      "created_by_user_id": "superadmin", "created_by_domain": "superadmin"}


def test_reserved_users_config(cfg, raw_config):
    assert cfg.directory.is_reserved("superadmin", "superadmin")
    assert cfg.directory.is_reserved("SuperAdmin", "SUPERADMIN")  # case-insensitive
    assert not cfg.directory.is_reserved("superadmin", "ANDREW")
    raw_config["directory"]["reserved_users"] = ["no-domain"]
    with pytest.raises(Exception, match="user_id/domain"):
        parse_config(raw_config)


def test_reserved_user_blocked_without_admin_token(client):
    # the test config has no admin token at all -> nobody may use superadmin here
    for action in ("preview", "submit"):
        res = client.post(f"/forms/user/{action}", json={"values": SUPERADMIN_CREATOR})
        assert res.status_code == 403
        err = res.json()["errors"][0]
        assert err["field"] == "created_by_user_id"
        assert "No admin token is configured" in err["message"]
    # project members are checked too
    member = {"project_id": "p1", "user_roles": [{"user_id": "superadmin", "domain": "superadmin", "role": "admin"}],
              **CREATOR}
    res = client.post("/forms/project/preview", json={"values": member})
    assert res.status_code == 403 and res.json()["errors"][0]["field"] == "user_roles.0.user_id"


def test_reserved_user_allowed_for_platform_admin(tmp_path, raw_config, monkeypatch):
    raw_config["builder"] = {"enabled": True, "admin_token": "adm1n"}
    raw_config["publish"]["dry_run"] = True
    path = tmp_path / "form-config.yaml"
    path.write_text(yaml.safe_dump(raw_config))
    for var, name in (("FORM_CONFIG_PATH", "form-config.yaml"), ("FORM_HISTORY_DB", "history.db"),
                      ("FORM_CUSTOM_FORMS", "custom_forms.json"), ("FORM_SETTINGS", "settings.json")):
        monkeypatch.setenv(var, str(tmp_path / name))

    import importlib

    import app.main as main

    importlib.reload(main)
    with TestClient(main.app) as c:
        denied = c.post("/forms/user/submit", json={"values": SUPERADMIN_CREATOR})
        assert denied.status_code == 403
        assert "Enter the admin token" in denied.json()["errors"][0]["message"]
        wrong = c.post("/forms/user/submit", json={"values": SUPERADMIN_CREATOR}, headers={"X-Admin-Token": "nope"})
        assert wrong.status_code == 403
        ok = c.post("/forms/user/submit", json={"values": SUPERADMIN_CREATOR}, headers={"X-Admin-Token": "adm1n"})
        assert ok.status_code == 200, ok.text


def test_reserved_users_hidden_from_picker(client, monkeypatch):
    import app.main as main

    monkeypatch.setattr(main.config_manager.get().directory, "rws_url", "http://rws.test:8000")
    monkeypatch.setattr(main, "directory_search_users", lambda cfg, s, n: [
        {"user_id": "superadmin", "domain": "superadmin", "name": "Super Admin"},
        {"user_id": "test_member", "domain": "ANDREW", "name": "S"},
    ])
    monkeypatch.setattr(main, "directory_get_user", lambda cfg, u, d: {"user_id": u, "domain": d})
    users = client.get("/directory/users", params={"search": ""}).json()["users"]
    assert [u["user_id"] for u in users] == ["test_member"]
    assert client.get("/directory/users/superadmin/superadmin").json()["reserved"] is True


def test_unregistered_user_blocks_submission(client, monkeypatch):
    import app.main as main
    from app import directory

    registered = {("test_creator", "TEST")}
    monkeypatch.setattr(main.config_manager.get().directory, "rws_url", "http://rws.test:8000")
    monkeypatch.setattr(
        directory, "get_user", lambda cfg, u, d: {"user_id": u, "domain": d} if (u, d) in registered else None
    )
    ghost = {"user_id": "u1", "domain": "ANDREW", "created_by_user_id": "ghost", "created_by_domain": "ANDREW"}
    for action in ("preview", "submit"):
        res = client.post(f"/forms/user/{action}", json={"values": ghost})
        assert res.status_code == 422, res.text  # refused, like a missing required field
        err = res.json()["errors"][0]
        assert err["field"] == "created_by_user_id" and "is not a registered user" in err["message"]
    assert client.get("/submissions").json() == []  # nothing was published or recorded

    ok = client.post("/forms/user/submit", json={"values": {"user_id": "u1", "domain": "ANDREW", **CREATOR}})
    assert ok.status_code == 200, ok.text


# --------------------------------------------------------------------------- sections + key types

SECTIONED_FORM = {
    "title": "Print job",
    "keys": [
        {"type": "section", "label": "Machine", "help": "Which printer and settings"},
        {"name": "printer", "label": "Printer", "type": "select", "options": ["MK4", "XL"], "required": True},
        {"name": "materials", "label": "Materials", "type": "multiselect", "options": ["PLA", "PETG", "TPU"]},
        {"type": "section", "label": "People"},
        {"name": "operator", "label": "Operator", "type": "person", "required": True},
        {"name": "contact", "label": "Contact", "type": "email"},
    ],
}


def test_yaml_section_needs_no_name_and_is_not_sent(raw_config):
    form = next(f for f in raw_config["forms"] if f["id"] == "user")
    form["fields"].insert(0, {"type": "section", "label": "Who", "help": "The person to register"})
    cfg = parse_config(raw_config)
    fields = cfg.get_form("user").fields
    assert fields[0].type == "section" and fields[0].name == "_section_0"
    payload = build_payload(cfg.get_form("user"), {"user_id": "u1", "domain": "ANDREW", **CREATOR}, cfg)
    assert not any(k.startswith("_section") for k in payload)


def test_yaml_section_rules(raw_config):
    no_label = copy.deepcopy(raw_config)
    next(f for f in no_label["forms"] if f["id"] == "user")["fields"].insert(0, {"type": "section"})
    with pytest.raises(Exception, match="needs a 'label'"):
        parse_config(no_label)

    in_list = copy.deepcopy(raw_config)
    project = next(f for f in in_list["forms"] if f["id"] == "project")
    members = next(f for f in project["fields"] if f["name"] == "user_roles")
    members["fields"].append({"type": "section", "label": "x"})
    with pytest.raises(Exception, match="sections can't be used inside a list"):
        parse_config(in_list)


def test_builder_sections_and_new_key_types(client):
    created = client.post("/builder/forms", json=SECTIONED_FORM)
    assert created.status_code == 200, created.text
    form_id = created.json()["form"]["id"]

    form = next(f for f in client.get("/config").json()["forms"] if f["id"] == form_id)
    shape = [(f["type"], f["name"]) for f in form["fields"]]
    assert shape[:8] == [
        ("text", "trial_id"),
        ("section", "_section_1"),
        ("select", "printer"),
        ("multiselect", "materials"),
        ("section", "_section_4"),
        ("text", "operator"),
        ("text", "operator_domain"),
        ("email", "contact"),
    ]
    sections = [f for f in form["fields"] if f["type"] == "section"]
    assert [(s["label"], s["help"]) for s in sections] == [
        ("Machine", "Which printer and settings"), ("People", None), ("Submitted by", None)
    ]
    names = [f["name"] for f in form["fields"]]
    assert names.index("created_by_user_id") == names.index(sections[-1]["name"]) + 1
    operator = next(f for f in form["fields"] if f["name"] == "operator")
    assert operator["lookup"] == "user" and operator["domain_field"] == "operator_domain"

    sent = client.post(f"/forms/{form_id}/submit", json={"values": {
        "trial_id": "t-1", "printer": "MK4", "materials": ["PLA", "TPU"],
        "operator": "test_operator", "operator_domain": "TEST", "contact": "a@b.edu", **CREATOR}})
    assert sent.status_code == 200, sent.text
    payload = sent.json()["payload"]
    assert payload["materials"] == ["PLA", "TPU"]
    assert (payload["operator"], payload["operator_domain"]) == ("test_operator", "TEST")
    assert not any(k.startswith("_section") for k in payload)

    bad = client.post(f"/forms/{form_id}/preview", json={"values": {
        "trial_id": "t-1", "printer": "MK4", "operator": "x", "operator_domain": "ANDREW", "contact": "not-an-email",
        **CREATOR}})
    assert bad.status_code == 422 and "contact" in {e["field"] for e in bad.json()["errors"]}


def test_builder_validation_for_new_types(client):
    def errors_for(keys):
        res = client.post("/builder/forms", json={"title": "x", "keys": keys})
        assert res.status_code == 422
        return res.text

    assert "needs a heading" in errors_for([{"type": "section", "label": ""}, {"name": "a"}])
    assert "sections alone hold no data" in errors_for([{"type": "section", "label": "Only a heading"}])
    assert "multiple choice needs at least one choice" in errors_for([{"name": "m", "type": "multiselect"}])
    # a person key 'created_by' would write the reserved created_by_domain
    assert "added automatically" in errors_for([{"name": "created_by", "type": "person"}])
    # a person key writes <key>_domain, which must not clash with another key
    assert "duplicate key" in errors_for([{"name": "op", "type": "person"}, {"name": "op_domain"}])


def test_old_saved_builder_forms_still_load():
    from app.custom_forms import BuilderForm

    old = {"id": "test", "title": "test", "description": "", "submit_label": "", "site": None, "keys": [
        {"name": "test", "label": "test", "type": "text", "required": False, "options": [], "default": None,
         "placeholder": None, "help": "", "min": None, "max": None}]}
    form = BuilderForm(**old).to_form()
    assert [f.name for f in form.fields] == [
        "trial_id", "test", "created_by_user_id", "created_by_domain", "timestamp"
    ]
