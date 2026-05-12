import json
import os
import uuid
import time
import argparse
import logging

import yaml

from mfi_ddb import BlobTopicFamily, Subscriber
from mfi_ddb.utils.script_utils import get_topic_from_config


# ---------------- LOGGING ----------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

logger = logging.getLogger("CFSService")


# ---------------- UTIL ----------------
def generate_uid():
    return str(uuid.uuid4())[-12:]


# ---------------- STORAGE ----------------
class LocalFileStorage:
    def __init__(self, config):
        self.save_dir = config.get('save_directory')
        if not self.save_dir:
            raise ValueError("save_directory is required")
        self.index_path = os.path.join(self.save_dir, "index.jsonl")

        os.makedirs(self.save_dir, exist_ok=True)
        logger.info(f"Storage initialized at: {self.save_dir}")

    def save_blob(self, topic, data):
        try:
            unique_id = generate_uid()

            # ---------- FILE ----------
            file_ext = data.get("file_type", "")
            if not file_ext.startswith("."):
                file_ext = f".{file_ext}"

            file_path = os.path.join(self.save_dir, f"{unique_id}{file_ext}")
            rel_file_path = os.path.relpath(file_path)

            with open(file_path, 'wb') as f:
                f.write(data["file"])

            logger.info(f"Saved file: {file_path}")

            # ---------- METADATA ----------
            metadata = data.copy()
            metadata.pop("file", None)
            metadata["topic"] = topic
            metadata["file_id"] = unique_id

            metadata_path = os.path.join(self.save_dir, f"{unique_id}.json")

            with open(metadata_path, 'w') as f:
                json.dump(metadata, f, indent=4)

            logger.info(f"Saved metadata: {metadata_path}")

            # ---------- INDEX ----------
            index_record = {
                "file_id": unique_id,
                "trial_id": metadata.get("trial_id"),
                "timestamp": metadata.get("timestamp"),
                "topic": topic,
                "file_type": metadata.get("file_type"),
            }

            self._append_to_index(index_record)

            return unique_id

        except Exception as e:
            logger.exception(f"Failed to save blob: {e}")
            return None

    def _append_to_index(self, record):
        try:
            with open(self.index_path, "a") as f:
                f.write(json.dumps(record) + "\n")
                f.flush()

            logger.debug(f"Indexed file_id: {record['file_id']}")

        except Exception:
            logger.exception("Failed to update index")


# ---------------- SERVICE ----------------
class CFSSubscriberService:
    def __init__(self, mqtt_config, cfs_config):
        self.mqtt_config = mqtt_config
        self.cfs_config = cfs_config

        self.subscriber = None

        if isinstance(mqtt_config, dict) and 'mqtt' in mqtt_config:
             self.subscriber = Subscriber(mqtt_config)
        else:
            logger.info("Running without MQTT subscriber (test mode)")
        self.storage = LocalFileStorage(cfs_config)

        self.running = False

    # ---------- LIFECYCLE ----------
    # def start(self):
    #     if not self.subscriber:
    #         logger.warning("Subscriber not initialized (test mode)")
    #         return
    #     topic = get_topic_from_config(self.cfs_config['topic'])

    #     logger.info(f"Subscribing to topic: {topic}")
    #     logger.info("Starting MQTT listener...")   

    #     self.subscriber.create_message_callback(
    #         topic, self._callback
    #     )

    #     self.subscriber.client.loop_start()
    #     self.running = True

    #     logger.info("CFS Subscriber Service started")
    def start(self):
        if not self.subscriber:
            logger.warning("Subscriber not initialized (test mode)")
            return
        
        topic = get_topic_from_config(self.cfs_config['topic'])

        # start loop FIRST so network traffic is processed
        self.subscriber.client.loop_start()
        
        # give loop a moment to stabilize
        time.sleep(1)

        logger.info(f"Subscribing to topic: {topic}")
        self.subscriber.create_message_callback(
            topic, self._callback
        )

        self.running = True
        logger.info("CFS Subscriber Service started")

    def stop(self):
        if not self.subscriber:
            return
        logger.info("Stopping service...")

        self.subscriber.client.loop_stop()
        self.subscriber.client.disconnect()

        self.running = False
        logger.info("Service stopped")

    # ---------- CALLBACK ----------
    # def _callback(self, message):
    #     topic = getattr(message, "topic", None)

    #     def parse():
    #         return BlobTopicFamily.process_message(message.payload)

    #     def get_handler(data_type):
    #         return {
    #             "attributes": self._handle_attributes,
    #             "data": self._handle_data,
    #         }.get(data_type)

    #     try:
    #         data_type, data = parse()
    #     except Exception as e:
    #         logger.exception(f"Failed to parse message: {e}")
    #         return

    #     logger.info(f"Received {data_type} message on topic: {topic}")

    #     handler = get_handler(data_type)
    #     if not handler:
    #         logger.warning(f"Unknown data type received: {data_type}")
    #         return

    #     try:
    #         handler(topic, data)
    #     except Exception as e:
    #         logger.exception(f"Error handling {data_type} message: {e}")

    def _callback(self, topic, payload):
        def parse():
            return BlobTopicFamily.process_message(payload)

        def get_handler(data_type):
            return {
                "attributes": self._handle_attributes,
                "data": self._handle_data,
            }.get(data_type)

        try:
            data_type, data = parse()
        except Exception as e:
            logger.exception(f"Failed to parse message: {e}")
            return

        logger.info(f"Received {data_type} message on topic: {topic}")

        handler = get_handler(data_type)
        if not handler:
            logger.warning(f"Unknown data type received: {data_type}")
            return

        try:
            handler(topic, data)
        except Exception as e:
            logger.exception(f"Error handling {data_type} message: {e}")
    # ---------- HANDLERS ----------
    def _handle_attributes(self, topic, data):
        try:
            if "description" not in data:
                logger.warning("Attributes message missing 'description'")
                return

            data['description']["topic"] = topic

            file_name = f"{data.get('trial_id', 'unknown')}_{data.get('timestamp', 'unknown')}.json"
            file_path = os.path.join(self.cfs_config['save_directory'], file_name)

            os.makedirs(self.cfs_config['save_directory'], exist_ok=True)

            with open(file_path, 'w') as f:
                json.dump(data['description'], f, indent=4)

            logger.info(f"Attributes saved: {file_path}")

        except Exception:
            logger.exception("Failed to handle attributes message")

    def _handle_data(self, topic, data):
        expected_keys = ["file_name", "file_type", "size", "timestamp", "file"]

        # --- minimal validation (same as your original intent) ---
        missing = [k for k in expected_keys if k not in data]
        if missing:
            logger.warning(f"Missing keys in data message: {missing}")
            return

        if not isinstance(data.get("file"), (bytes, bytearray)):
            logger.warning("Received file is not bytes")
            return

        self.storage.save_blob(topic, data)


# ---------------- MAIN ----------------
def main(mqtt_cfg_file, cfs_cfg_file):
    logger.info(f"Using MQTT config: {os.path.abspath(mqtt_cfg_file)}")
    logger.info(f"Using CFS config: {os.path.abspath(cfs_cfg_file)}")

    with open(mqtt_cfg_file) as f:
        mqtt_config = yaml.safe_load(f)

    with open(cfs_cfg_file) as f:
        cfs_config = yaml.safe_load(f)

    service = CFSSubscriberService(mqtt_config, cfs_config)

    try:
        service.start()

        while True:
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")

    finally:
        service.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Subscribe to MQTT topic and save blobs to local storage"
    )
    parser.add_argument("mqtt_config_path")
    parser.add_argument("cfs_config_path")

    args = parser.parse_args()

    main(args.mqtt_config_path, args.cfs_config_path)