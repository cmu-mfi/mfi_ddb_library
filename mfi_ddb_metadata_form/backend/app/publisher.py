"""
Publish a validated KV payload through the mfi_ddb KV data adapter.

Each submission is one short-lived adapter session, exactly as if someone ran
the library by hand with a KeyValue adapter config:

    KvDataAdapter({"adapter_name", "trial_id", "payloads": [payload]})
      -> Streamer(streamer_cfg with topic_family="kv")
         (connects, publishes birth metadata + the payload)
      -> Streamer.disconnect()  (publishes death metadata)

The payload lands on  mfi-v1.0-kv/<enterprise>/<site>/<msg_type>/data,
where the KV / metadata database nodes pick it up.

Two safety nets around the library:
  * a preflight MQTT connect with a timeout - the library's own connect loop
    waits forever if the broker refuses the connection;
  * a process-wide lock so concurrent submissions publish one at a time.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from typing import Optional

from app.config import FormConfig, streamer_config_for, topic_prefix

logger = logging.getLogger(__name__)

_publish_lock = threading.Lock()


class PublishError(Exception):
    pass


def topic_for(cfg: FormConfig, msg_type: str, site: Optional[str] = None) -> str:
    return f"{topic_prefix(cfg, site)}/{msg_type}/data"


def session_trial_id(payload: dict, cfg: FormConfig) -> str:
    trial_id = payload.get("trial_id")
    return str(trial_id) if trial_id else cfg.adapter.trial_id


def session_user(payload: dict, cfg: FormConfig) -> dict:
    """User the adapter session (birth/death trial) is recorded under.

    Matches the submission's created_by_user_id / created_by_domain when the
    form has them, so the session belongs to whoever is entered as the
    creator. Otherwise the app's identity (streamer.user) is used.
    """
    user = copy.deepcopy(cfg.streamer.get("user", {}))
    user_id = payload.get("created_by_user_id")
    domain = payload.get("created_by_domain")
    if user_id and domain and (user_id, domain) != (user.get("user_id"), user.get("domain")):
        # name/email in streamer.user describe the app identity, not this user
        user = {"user_id": str(user_id), "domain": str(domain), "email": "", "name": ""}
    return user


def build_adapter_config(payload: dict, cfg: FormConfig) -> dict:
    return {
        "adapter_name": cfg.adapter.adapter_name,
        "trial_id": session_trial_id(payload, cfg),
        "payloads": [copy.deepcopy(payload)],
    }


def check_broker(cfg: FormConfig, timeout: Optional[float] = None) -> None:
    """Connect once with a timeout and the same credentials the library will use."""
    import paho.mqtt.client as mqtt

    mqtt_cfg = cfg.streamer["mqtt"]
    host = mqtt_cfg["broker_address"]
    port = int(mqtt_cfg.get("broker_port", 1883))
    timeout = timeout or cfg.publish.connect_timeout_seconds

    result: dict = {}
    done = threading.Event()

    def on_connect(_client, _userdata, _flags, reason_code, _properties=None):
        result["rc"] = reason_code
        done.set()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.connect_timeout = timeout
    client.username_pw_set(mqtt_cfg.get("username", None), mqtt_cfg.get("password", None))
    try:
        client.connect(host, port, keepalive=10)
    except Exception as err:
        raise PublishError(f"Cannot reach MQTT broker {host}:{port}: {err}") from None

    client.loop_start()
    try:
        if not done.wait(timeout):
            raise PublishError(f"Timed out after {timeout:g}s connecting to MQTT broker {host}:{port}")
        rc = result["rc"]
        if getattr(rc, "is_failure", False) or (isinstance(rc, int) and rc != 0):
            raise PublishError(f"MQTT broker {host}:{port} refused the connection: {rc}")
    finally:
        client.disconnect()
        client.loop_stop()


def publish(payload: dict, cfg: FormConfig, site: Optional[str] = None) -> dict:
    """Publish one payload under `site` (default: streamer.mqtt.site).

    Returns {"status", "topic", "trial_id"}; raises PublishError.
    """
    topic = topic_for(cfg, payload["msg_type"], site)
    trial_id = session_trial_id(payload, cfg)

    if cfg.publish.dry_run:
        logger.info("[dry-run] would publish %s payload to %s", payload["msg_type"], topic)
        return {"status": "dry_run", "topic": topic, "trial_id": trial_id}

    wait = cfg.publish.connect_timeout_seconds + cfg.publish.settle_seconds + 5
    if not _publish_lock.acquire(timeout=wait):
        raise PublishError("Another submission is still being published - try again shortly.")
    try:
        check_broker(cfg)

        from mfi_ddb import Streamer
        from mfi_ddb.data_adapters import KvDataAdapter

        try:
            adapter = KvDataAdapter(build_adapter_config(payload, cfg))
        except Exception as err:
            raise PublishError(f"KV adapter rejected the configuration: {err}") from None
        if not adapter.component_ids:
            raise PublishError(
                "KV adapter dropped the payload (msg_type 'data' needs a trial_id other "
                f"than '{cfg.adapter.trial_id}')."
            )

        try:
            streamer_cfg = streamer_config_for(cfg, site)
            streamer_cfg["user"] = session_user(payload, cfg)
            streamer = Streamer(streamer_cfg, adapter, stream_on_update=False)
        except Exception as err:
            raise PublishError(f"Streamer failed to publish: {err}") from None

        try:
            # Give paho's network thread time to flush the QoS1 publishes
            # before the client loop is stopped by disconnect().
            time.sleep(cfg.publish.settle_seconds)
        finally:
            streamer.disconnect()

        logger.info("Published %s payload to %s (trial_id=%s)", payload["msg_type"], topic, trial_id)
        return {"status": "published", "topic": topic, "trial_id": trial_id}
    finally:
        _publish_lock.release()
