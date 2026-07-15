import json
import os
import uuid
import time  # <-- 1. Ensure time is imported (already there)
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


from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter # <-- 2. Added get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
meter = get_meter("mfi.blob.connector")

blob_messages_counter = meter.create_counter(
    "mfi_blob_messages_processed_total",
    description="Total blob messages processed by the connector"
)

blob_write_duration = meter.create_histogram(
    "mfi_blob_write_duration_seconds",
    description="Time taken to save blob and metadata to local disk"
)

blob_save_errors = meter.create_counter(
    "mfi_blob_save_errors_total",
    description="Total failures encountered while writing blobs to disk"
)

blob_size_histogram = meter.create_histogram(
    "mfi_blob_size_bytes",
    description="Size distribution of successfully saved files in bytes"
)
# ==========================================


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
        start_time = time.perf_counter()  # <-- 3. Start I/O timer
        try:
            unique_id = generate_uid()

            # ---------- FILE ----------
            file_ext = data.get("file_type", "")
            if not file_ext.startswith("."):
                file_ext = f".{file_ext}"

            file_path = os.path.join(self.save_dir, f"{unique_id}{file_ext}")

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

            # --- RECORD METRICS ON SUCCESS ---
            duration = time.perf_counter() - start_time
            blob_write_duration.record(duration)
            
            # Record size from incoming metadata or the byte length
            file_size = len(data.get("file", b""))
            blob_size_histogram.record(file_size, {"file_type": file_ext})

            return unique_id

        except Exception as e:
            # --- RECORD METRICS ON FAILURE ---
            blob_save_errors.add(1)
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
    def start(self):
        if not self.subscriber:
            logger.warning("Subscriber not initialized, running in test mode — storage only")
            self.running = False
            return

        topic = get_topic_from_config(self.cfs_config['topic'])

        self.subscriber.client.loop_start()
        time.sleep(1)

        logger.info(f"Subscribing to topic: {topic}")
        self.subscriber.create_message_callback(topic, self._callback)

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
            # --- COUNT INCOMING RAW MESSAGES ---
            blob_messages_counter.add(1, {"type": data_type})
            
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
    logger.info(f"Using unified config file: {os.path.abspath(mqtt_cfg_file)}")

    # Load the single file
    with open(mqtt_cfg_file) as f:
        unified_config = yaml.safe_load(f)

    # Extract the separate blocks from the single dictionary
    mqtt_config = unified_config # keeping mqtt accessible for the subscriber service
    cfs_config = unified_config.get('config')

    service = CFSSubscriberService(mqtt_config, cfs_config)

    try:
        service.start()
        if not service.running:
            logger.error("Service failed to start")
            return
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

    parser.add_argument("config_path", help="Path to the unified config file for blob")

    args = parser.parse_args()

    main(args.config_path, args.config_path)