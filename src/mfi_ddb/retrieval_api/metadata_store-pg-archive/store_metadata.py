"""MQTT -> metadata store bridge.

This script demonstrates subscribing to a blob topic, extracting an "attributes"
payload and inserting a trial row into PostgreSQL using the helper functions in
`metadata_utils.py`.

Usage:
    python store_metadata.py <mqtt_cfg.yaml> <metadata_cfg.yaml>

The metadata config is expected to contain a `topic` field like other store_* scripts.
This is a small example — adapt field mapping to your real message schema.
"""
from typing import Any, Dict
import os
import argparse
import yaml
import time
from datetime import datetime, timezone
import json
import re
import zipfile

from mfi_ddb import BlobTopicFamily, Subscriber
from mfi_ddb.utils.script_utils import get_topic_from_config

import metadata_utils as mu


def _handle_attributes_message(data: Dict[str, Any]):
    """Map the blob attributes message into a trial insert and call metadata_utils.
    """
    # Helper to parse ISO timestamps safely
    def _to_epoch_seconds(iso_ts: Any) -> int:
        if isinstance(iso_ts, (int, float)):
            return int(iso_ts)
        if isinstance(iso_ts, str):
            try:
                dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return int(dt.timestamp())
            except Exception:
                pass
        return int(time.time())

    adapter = data.get("adapter", {}) or {}
    broker = data.get("broker", {}) or {}
    #broker_mqtt = broker.get("mqtt", {}) or {}
    time_block = data.get("time", {}) or {}

    # trial_name priority: adapter.config.trial_id -> adapter.attributes.trial_id -> top-level trial_id
    trial_id = (adapter.get("config", {}) or {}).get("trial_id")

    # birth timestamp from ISO string, fallback to now
    timestamp = _to_epoch_seconds(time_block.get("birth"))

    # user/project mapping based on available fields
    #user_id = broker_mqtt.get("username") or "unknown_user"
    #user_domain = broker_mqtt.get("user_domain") or "unknown_domain"

    # Store entire incoming payload as metadata for traceability
    metadata = data

    if not trial_id:
        print("No trial_id found in message; skipping insert/update")
        return

    # If death timestamp is present, update the trial end; else treat as birth insert
    death_iso = (data.get("time", {}) or {}).get("death")
    if death_iso:
        death_ts = _to_epoch_seconds(death_iso)
        try:
            row = mu.update_trial_detail_end(
                trial_name=trial_id,
                death_timestamp=death_ts,
                clean_exit=True,
            )
            if row:
                print("Updated trial (death):", row)
            else:
                print("No trial row found to update for trial_id:", trial_id)
        except Exception as exc:
            print("Error updating trial end detail:", exc)
        return

    try:
        row = mu.insert_trial_detail(
            trial_name=trial_id,
            #user_id=user_id,
            #user_domain=user_domain,
            birth_timestamp=timestamp,
            metadata=metadata,
        )
        print("Inserted trial row:", row)
    except Exception as exc:
        print("Error inserting trial detail:", exc)


def callback(config, message):
    """Parse incoming blob payloads and act on 'attributes' messages."""
    payload_type, data = BlobTopicFamily.process_message(message)
    print(f"Received {payload_type} message with keys: {list(data.keys())}")

    if payload_type == "attributes":
        _handle_attributes_message(data)
    else:
        print("Received non-attributes blob message; ignoring for metadata insert.")


def _strip_jsonc_comments(text: str) -> str:
    """Remove // and /* */ comments from JSONC-like text for safe json.loads."""
    # Remove // comments
    text = re.sub(r"//.*", "", text)
    # Remove /* ... */ comments (including multiline)
    text = re.sub(r"/\*.*?\*/", "", text, flags=re.S)
    return text


def _process_zip(zip_path: str):
    print(f"Processing zip file: {os.path.abspath(zip_path)}")
    if not os.path.isfile(zip_path):
        print("Zip path does not exist or is not a file:", zip_path)
        return

    with zipfile.ZipFile(zip_path, 'r') as zf:
        names = [n for n in zf.namelist() if not n.endswith('/')]
        if not names:
            print("Zip archive is empty")
            return
        for name in names:
            if not (name.lower().endswith('.json') or name.lower().endswith('.jsonc')):
                continue
            try:
                with zf.open(name, 'r') as fh:
                    raw = fh.read().decode('utf-8')
                if name.lower().endswith('.jsonc'):
                    raw = _strip_jsonc_comments(raw)
                payload = json.loads(raw)
            except Exception as exc:
                print(f"Failed to parse {name}: {exc}")
                continue

            print(f"Ingesting {name}")
            _handle_attributes_message(payload)


def main(mqtt_cfg_file: str, metadata_cfg_file: str):
    print(f"Using MQTT config file: {os.path.abspath(mqtt_cfg_file)}")
    print(f"Using metadata config file: {os.path.abspath(metadata_cfg_file)}")

    mqtt_config = yaml.safe_load(open(mqtt_cfg_file))
    metadata_config = yaml.safe_load(open(metadata_cfg_file))

    # Initialize subscriber and subscribe to configured topic
    mqtt_sub = Subscriber(mqtt_config)
    topic = get_topic_from_config(metadata_config["topic"])
    mqtt_sub.create_message_callback(topic, lambda message: callback(metadata_config, message))

    # Run the mqtt loop in background and wait until interrupted
    try:
        mqtt_sub.client.loop_start()
        print("Listening for metadata messages (press Ctrl+C to stop)")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Interrupted; shutting down")
    finally:
        mqtt_sub.client.loop_stop()
        mqtt_sub.client.disconnect()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest metadata either from MQTT or a zip of JSON payloads.")
    parser.add_argument("mqtt_config_path", nargs='?', help="Path to MQTT configuration YAML (e.g., mqtt.yaml)")
    parser.add_argument("metadata_config_path", nargs='?', help="Path to metadata configuration YAML (e.g., mtconnect.yaml)")
    parser.add_argument("--zip", dest="zip_path", help="Path to a zip containing birth/death JSON/JSONC payloads for bulk upload")
    args = parser.parse_args()

    if args.zip_path:
        _process_zip(args.zip_path)
    else:
        if not args.mqtt_config_path or not args.metadata_config_path:
            parser.error("When --zip is not provided, mqtt_config_path and metadata_config_path are required.")
        main(args.mqtt_config_path, args.metadata_config_path)