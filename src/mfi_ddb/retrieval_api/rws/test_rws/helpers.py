import copy
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Tuple, Optional

import paho.mqtt.client as mqtt

logger = logging.getLogger(__name__)


def publish(client: mqtt.Client, payload: dict):
    device = "test_device"
    topic_prefix = f"mfi-v1.0-kv/CMU/{device}"
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


def insert_user(client, user: Tuple[str, str]):
    # PUBLISH DATA
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "user",
        "user_id": user[0],
        "domain": user[1],
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso(),
    }

    publish(client, payload)
    grace_wait()


def insert_trial(
    client: mqtt.Client,
    user: Tuple[str, str],
    time_period_sec: float,
    trial_name: str = "",
    project_name: str = "",
    custom_dict_payload: Optional[dict] = None
) -> str:
    if trial_name == "":
        trial_name = uid("test_")
    birth_payload = get_payload_template("birth")
    birth_payload["trial_id"] = trial_name
    birth_payload["time"]["birth"] = now_iso()
    birth_payload["user"]["user_id"] = user[0]
    birth_payload["user"]["domain"] = user[1]

    if custom_dict_payload is not None:
        birth_payload = birth_payload | custom_dict_payload

    if project_name != "" and project_name is not None:
        birth_payload["project"]["name"] = project_name

    publish(client, birth_payload)
    time.sleep(time_period_sec)

    death_payload = copy.deepcopy(birth_payload)
    death_payload["msg_type"] = "death"
    death_payload["time"]["death"] = now_iso()

    publish(client, death_payload)
    grace_wait()

    return birth_payload["trial_id"]


def insert_user_roles(client, user: Tuple[str, str], project_name: str, role: str):

    project_payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": project_name,
        "user_roles": [{"user_id": user[0], "domain": user[1], "role": role}],
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso(),
    }

    publish(client, project_payload)
    grace_wait()


def tp_tag_by_project_name(
    client: mqtt.Client,
    trial_name: str,
    project_name: str,
    start_time: str,
    user_operator: Tuple[str, str],
):

    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "tp-tag",
        "trial_id": trial_name,
        "project_name": project_name,
        "time_start": start_time,
        "time_end": now_iso(),
        "trial_user_id": user_operator[0],
        "trial_user_domain": user_operator[1],
        "created_by_user_id": user_operator[0],
        "created_by_domain": user_operator[1],
        "timestamp": now_iso(),
    }

    publish(client, payload)
    grace_wait()
