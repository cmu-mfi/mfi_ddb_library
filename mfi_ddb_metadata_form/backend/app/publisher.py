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

from app.config import FormConfig, topic_prefix

logger = logging.getLogger(__name__)

_publish_lock = threading.Lock()


class PublishError(Exception):
    pass


def topic_for(cfg: FormConfig, msg_type: str) -> str:
    return f"{topic_prefix(cfg)}/{msg_type}/data"


def session_trial_id(payload: dict, cfg: FormConfig) -> str:
    trial_id = payload.get("trial_id")
    return str(trial_id) if trial_id else cfg.adapter.trial_id


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


def publish(payload: dict, cfg: FormConfig) -> dict:
    """Publish one payload. Returns {"status", "topic", "trial_id"}; raises PublishError."""
    topic = topic_for(cfg, payload["msg_type"])
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
            streamer = Streamer(copy.deepcopy(cfg.streamer), adapter, stream_on_update=False)
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
