import json
import logging
import os
import time
import uuid
from datetime import datetime, timedelta, timezone

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)

def publish(client: mqtt.Client, payload: dict):
    device = 'test_device'
    topic_prefix = f"mfi-v1.0-kv/CMU/{device}"
    logger.debug(f"Publishing to device: {device}")
    payload = {"metadata": payload}
    for key in payload.keys():
        if isinstance(payload[key], dict):
            payload[key] = json.dumps(payload[key])
        client.publish(topic=f"{topic_prefix}/{key}", payload=payload[key], qos=1)
        logger.debug(f"Published data on topic: {topic_prefix}/{key}")

def get_payload_template(type: str):
    current_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = f"../../metadata_store_pg/json_payload_examples/{type}.json"
    with open(os.path.join(current_dir, file_path), "r") as f:
        data = json.load(f)

    return data

def grace_wait():
    # WAIT TO MAKE SURE THE DATA IS IN THE MDS DB
    time.sleep(0.5)

def uid(prefix: str = "") -> str:
    """Return a short collision-proof string, optionally prefixed."""
    return f"{prefix}{uuid.uuid4().hex[:10]}"

def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()
