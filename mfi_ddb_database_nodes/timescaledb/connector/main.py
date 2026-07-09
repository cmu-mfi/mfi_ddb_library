"""TimeScaleDB connector: MQTT historian -> Sparkplug B decode -> DB insert."""

import json
import time
import queue
import threading
from pathlib import Path
from unicodedata import name
import yaml
from typing import Any, List, Tuple, Optional
import paho.mqtt.client as mqtt
from paho.mqtt.enums import CallbackAPIVersion
from datetime import datetime, timezone
from mqtt_spb_wrapper.spb_base import SpbPayloadParser, SpbTopic

from db import TimeScaleWriter

# Configuration and initialization
def load_config(path: Path):
    """Load YAML config for MQTT and TimeScaleDB settings."""
    with path.open("r") as f:
        return yaml.safe_load(f)

# Load configuration
CONFIG_PATH = Path(__file__).with_name("config.yaml")
cfg = load_config(CONFIG_PATH)

# `writer` is created inside `main()` to avoid opening a DB socket at import time
# which makes testing and other import-time operations brittle.
writer = None

# Thread safe queue to pass metrics from MQTT thread to DB worker thread
# Limit the queue to 50,000 message chunks to prevent memory bloat if the DB drops
data_queue: queue.Queue = queue.Queue(maxsize=50000)


# parsing and classification logic
def decode_sparkplug(payload_bytes: bytes) -> List[Tuple[str, Any, Optional[int]]]:
    """Decode Sparkplug B bytes into (metric_name, value, timestamp_ms) tuples.

    The wrapper parses the protobuf payload into a dict. We only keep metrics
    that have a name, and return the timestamp in epoch milliseconds if present.
    """
    parser = SpbPayloadParser()
    payload = parser.parse_payload(payload_bytes)

    if not isinstance(payload, dict):
        return []
    if "metrics" not in payload:
        return []

    metrics: List[Tuple[str, Any, Optional[int]]] = []
    for item in payload["metrics"]:
        # Sparkplug metrics are dicts with name/value/timestamp.
        name = item.get("name")
        value = item.get("value")
        ts = item.get("timestamp")
        if name is not None:
            metrics.append((name, value, ts))
    return metrics

def to_ts(ts_ms) -> datetime:
    """Convert epoch milliseconds to a timezone-aware datetime.

    Accepts ints/floats or numeric strings. Missing or invalid values fall back
    to the current UTC time to keep inserts robust.
    """
    if ts_ms is None:
        return datetime.now(timezone.utc)
    if isinstance(ts_ms, str):
        try:
            ts_ms = float(ts_ms)
        except ValueError:
            return datetime.now(timezone.utc)
    return datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)

def classify_value(v) -> Tuple[Optional[float], Optional[str], Optional[str]]:
    """Map a metric value into the numeric/text/JSON columns.

    - bool/int/float -> value_num
    - dict/list -> value_json
    - everything else -> value_text
    """
    if isinstance(v, bool):
        return (float(int(v)), None, None)
    if isinstance(v, (int, float)):
        return (float(v), None, None)
    if isinstance(v, (dict, list)):
        return (None, None, json.dumps(v))
    return (None, str(v), None)


# MQTT Callback - the producer
def on_connect(client, userdata, flags, rc, properties=None):
    """MQTT callback: handle connection."""
    if rc == 0:
        print(f"Connected to MQTT broker successfully")
    else:
        print(f"Connection failed with result code {rc}")

def on_message(client, userdata, msg):
    """MQTT callback: decode payload and batch insert into TimeScaleDB."""
    try:
        # initialize topic parser
        # spb_topic = SpbTopic(msg.topic)
        
        # not using the sbptopic parser since it is harcoded to expect a namespace
        # instead I can drop the namespace directly from the array
        topic_fields = msg.topic.split("/")

        if len(topic_fields) < 3:
            raise ValueError(f"Malformed MQTT topic structure: {msg.topic}")

        # extract the message type string
        msg_type = topic_fields[2]

        # delete the message type from the array
        del topic_fields[2]

        # assembling the topic path, leaving out message type folder
        # clean_fields = [spb_topic.namespace, spb_topic.domain_name]

        # if spb_topic.eon_name:
        #     clean_fields.append(spb_topic.eon_name)
        # if spb_topic.eon_device_name:
        #     clean_fields.append(spb_topic.eon_device_name)
        
        clean_topic = "/".join(topic_fields)
        
        metrics = decode_sparkplug(msg.payload)
        rows = []
        for name, value, ts_ms in metrics:
            t = to_ts(ts_ms)
            
            value_num, value_text, value_json = classify_value(value)

            # prepend the message type context into the existing metric string
            tracked_metric_name = f"{msg_type}/{name}"
            
            clean_name = name.lstrip("DATA/") if name.startswith("DATA/") else name

            full_topic_path = f"{clean_topic}/{clean_name}"

            # Persist each metric as a separate row keyed by topic + component + metric.
            rows.append(
                (t, full_topic_path, cfg["component_id"], tracked_metric_name, value_num, value_text, value_json)
            )
        if rows:
            # writer.insert_rows(rows) --> would overwhelm the DB if we do it directly in the MQTT thread; instead, push to the queue for the background worker to consume
            try:
                data_queue.put(rows, block=False)  # Non-blocking put; raises queue.Full if the worker is too far behind
            except queue.Full:
                print(f"CRITICAL: Data queue is full! Dropping {len(rows)} metrics from topic {msg.topic}")
    except Exception as e:
        print(f"Error processing MQTT message incoming on topic {msg.topic}: {e}")


# --- Background Worker (Consumer) ---

def db_batch_writer_worker(
    q: queue.Queue, db_writer: TimeScaleWriter, max_batch_size=1000, flush_interval_sec=0.1
):
    """Background loop that collects rows from the queue and flushes them to TimescaleDB

    in micro-batches. Flushes occur when max_batch_size is reached OR flush_interval_sec expires.
    """
    print("Background DB Batch Worker Thread started.")
    while True:
        batch = []
        start_time = time.time()

        # Gather rows until max_batch_size or flush_interval is met
        while len(batch) < max_batch_size:
            elapsed_time = time.time() - start_time
            remaining_time = flush_interval_sec - elapsed_time

            if remaining_time <= 0:
                break

            try:
                # Wait for rows to arrive up to the remaining time boundary
                # Each item from the queue is a list of rows from a single MQTT message
                message_rows = q.get(timeout=max(remaining_time, 0.001))
                batch.extend(message_rows)
                q.task_done()
            except queue.Empty:
                # Timeout hit with no new items; break out and flush current batch
                break

        # Commit the accumulated micro-batch to the hypertable
        if batch:
            # Loop until successful so we don't lose data during brief DB drops
            while True:
                try:
                    # If connection was previously broken, try to reset it
                    if db_writer.is_closed():
                        print("DB connection closed. Attempting reconnect...")
                        db_writer.reconnect()

                    db_writer.insert_rows(batch)
                    break  # Success! Break the retry loop and move to next batch
                except Exception as e:
                    print(f"Database insertion error (Size: {len(batch)}): {e}. Retrying in 5s...")
                    # Force-close the socket so subsequent loop iterations notice the closed state
                    try:
                        conn = getattr(db_writer, "conn", None)
                        if conn is not None:
                            conn.close()
                    except Exception:
                        pass
                    time.sleep(5)


def main():
    """Start the background batch worker, connect to broker, and process events."""
    # 1. Create DB writer instance and spin up the background consumer thread
    
    # db_writer = TimeScaleWriter(cfg["timescaledb"])

    # adding retries to ensure that the db is up and the schema is initialized before we start the MQTT client and worker thread
    # max of a 45 second wait before application gives up and exits.
    db_writer = None
    retry_count = 0
    max_retries = 15
    
    print("Connecting to TimescaleDB and validating/initializing schema...")
    while db_writer is None:
        try:
            db_writer = TimeScaleWriter(cfg["timescaledb"])
        except Exception as e:
            retry_count += 1
            if retry_count > max_retries:
                print("CRITICAL: Could not connect to TimescaleDB or verify schema. Exiting application.")
                raise e
            print(f"Database/Schema not ready yet ({e}). Retrying in 3 seconds... ({retry_count}/{max_retries})")
            time.sleep(3)

    worker_thread = threading.Thread(
        target=db_batch_writer_worker,
        args=(data_queue, db_writer),
        kwargs={"max_batch_size": 1000, "flush_interval_sec": 0.1},
        daemon=True,  # Allows clean application termination
    )
    worker_thread.start()

    # 2. Setup and connect the MQTT broker client
    mqtt_cfg = cfg["mqtt"]
    client = mqtt.Client(CallbackAPIVersion.VERSION2)
    if mqtt_cfg.get("username"):
        client.username_pw_set(mqtt_cfg["username"], mqtt_cfg.get("password", ""))

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(mqtt_cfg["broker_address"], mqtt_cfg.get("broker_port", 1883), 60)
    client.subscribe(mqtt_cfg["topic"])

    print(f"MQTT Client subscribed to {mqtt_cfg['topic']}. Listening for messages...")
    client.loop_forever()

if __name__ == "__main__":
    main()