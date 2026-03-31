import json
import os
from configparser import ConfigParser
import logging
from datetime import datetime

from mfi_ddb import KeyValueTopicFamily, Subscriber
from mfi_ddb.utils.script_utils import get_topic_from_config
from pg_mds import MdsConnector
from pg_config import load_config

logging.basicConfig(level=logging.INFO)
mds = MdsConnector()


def callback(config, topic, message):
    data: dict = KeyValueTopicFamily.process_message(message)
    logging.info(f"Received message with keys: {data.keys()} on topic: {topic}")

    # TODO ----------------------------------------------------------------------
    # streamer.py uses "metadata" tags for the key-value metadata,
    # but that template is not exactly defined in the kv schema.
    # Please review the data structure and adjust schema/streamer code as needed.
    # in case of streamer.py changes, store_cfs.py needs to be updated as well.
    # Consider message type in the schema. It can be payload driven or topic driven.
    # ---------------------------------------------------------------------- TODO

    # 1. DETERMINE MESSAGE TYPE
    # ===========================================================================
    # Message types: birth, death, user, project, trial-project-tag, others
    msg_type = data["msg_type"]

    # 2. UPDATE MDS TABLES BASED ON MESSAGE TYPE
    # ===========================================================================
    # populate function parameters based on the message content.

    if msg_type == "birth":
        metadata_exclude_keys = ["schema_version", "msg_type", "time", "trial_id"]
        mds.insert_trial(
            trial_id=data["trial_id"],
            user_id=data["user"]["user_id"],
            user_domain=data["user"]["domain"],
            project_id=data.get("project", {}).get("project_id", None),
            # TODO: project_name can be used to get project_id if project_id is not provided. 
            # This requires an additional query to the project table in the MDS.
            birth_timestamp=data["time"]["birth"],
            metadata={k: v for k, v in data.items() if k not in metadata_exclude_keys},
        )
    elif msg_type == "death":
        if "death" not in data["time"]:
            death_timestamp = datetime.now().isoformat()  # Use current time as fallback
            clean_exit = False  # Assume unclean exit if death timestamp is not provided
        else:
            death_timestamp = data["time"]["death"]
            clean_exit = True
        metadata_exclude_keys = ["schema_version", "msg_type", "time", "trial_id"]
        mds.update_trial(
            trial_id=data["trial_id"],
            death_timestamp=death_timestamp,
            clean_exit=clean_exit,
            metadata={k: v for k, v in data.items() if k not in metadata_exclude_keys},
        )
    elif msg_type == "user":
        mds.insert_user(
            user_id=data["user_id"],
            domain=data["domain"],
            email=data["email"] if "email" in data else "",
            name=data["name"] if "name" in data else "",
            created_by_user_id=data["created_by_user_id"],
            created_by_domain=data["created_by_domain"],
            timestamp=data["timestamp"]
        )
    elif msg_type == "project":
        exclude_keys = [
            "schema_version", 
            "msg_type", 
            "project_id", 
            "project_name",
            "created_by_user_id",
            "created_by_domain",
            "timestamp"
        ]
        mds.insert_project(
            project_id=data["project_id"] if "project_id" in data else "",
            name=data["project_name"] if "project_name" in data else "",
            created_by_user_id=data["created_by_user_id"],
            created_by_domain=data["created_by_domain"],
            timestamp=data["timestamp"],
            details={k: v for k, v in data.items() if k not in exclude_keys}
        )
    elif msg_type == "tp-tag":
        mds.update_trial_project(
            trial_id=data["trial_id"],
            user_id=data["trial_user_id"],
            user_domain=data["trial_user_domain"],
            project_id=data["project_id"],
            # TODO: project_name can be used to get project_id if project_id is not provided. 
            # This requires an additional query to the project table in the MDS.            
            time_start=data["time_start"],
            time_end=data["time_end"],
            created_by_user_id=data["created_by_user_id"],
            created_by_domain=data["created_by_domain"],
            timestamp=data["timestamp"]
        )
    elif msg_type == "data":
        pass
    else:
        logging.info(f"Unknown message type: {msg_type} with topic: {topic}")


def main():
    # LOAD CONFIG
    config_file = "broker.ini"
    mqtt_config = load_config(config_file, section="mqtt")

    # INIT A CONNECTION
    mqtt_sub = Subscriber(mqtt_config)

    # SUBSCRIBE TO TOPIC AND SET A CALLBACK
    topic_config = load_config(config_file, section="topic")
    topic = get_topic_from_config(topic_config)
    mqtt_sub.create_message_callback_with_topic(
        topic, lambda full_topic, message: callback(mqtt_config, full_topic, message)
    )

    mqtt_sub.client.loop_start()

    while KeyboardInterrupt:
        pass

    mqtt_sub.client.loop_stop()
    mqtt_sub.client.disconnect()


if __name__ == "__main__":
    main()
