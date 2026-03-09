import json
import os
from configparser import ConfigParser
import logging

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

    # 1. TODO: DETERMINE MESSAGE TYPE
    # ===========================================================================
    # Message types: birth, death, user, project, trial-project-tag, others
    msg_type = ...
    ...

    # 2. UPDATE MDS TABLES BASED ON MESSAGE TYPE
    # ===========================================================================
    # TODO: populate function parameters based on the message content.

    if msg_type == "birth":
        mds.insert_trial(
            trial_name=...,
            user_id=...,
            user_domain=...,
            project_id=...,
            birth_timestamp=...,
            metadata=...,
        )
    elif msg_type == "death":
        mds.update_trial(
            trial_id=...,
            death_timestamp=...,
            clean_exit=...,
            metadata=...,
        )
    elif msg_type == "user":
        mds.insert_user(
            user_id=...,
            domain=...,
            email=...,
            name=...,
            created_by=...,
        )
    elif msg_type == "project":
        mds.insert_project(
            project_id=...,
            name=...,
            details=...,
            created_by=...,
        )
    elif msg_type == "trial-project-tag":
        mds.update_trial_project(
            trial_id=...,
            user_id=...,
            user_domain=...,
            project_id=...,
            time_start=...,
            time_end=...,
        )
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
