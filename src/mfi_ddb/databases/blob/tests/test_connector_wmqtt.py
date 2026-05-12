import sys
import os
import time
import logging
from mfi_ddb.databases.blob.connector import CFSSubscriberService

logging.basicConfig(level=logging.DEBUG)

def test_mqtt_ingestion_live():
    test_dir = "./test_output"
    os.makedirs(test_dir, exist_ok=True)
    files_before = set(os.listdir(test_dir))
    print(f"\nFiles before test: {files_before}")

    mqtt_config = {
        "mqtt": {
            "broker_address": "128.128.128.128",
            "broker_port": 1883,
            "enterprise": "",
            "site": "",
            "username": "username",
            "password": "password",
            "tls_enabled": False,
            "debug": False
        }
    }
    cfs_config = {
        "save_directory": "",
        "topic": {
            "version": "",
            "topic_family": "",
            "enterprise": "",
            "site": None,
            "area": None,
            "device": None
        }
    }

    # ---------- START SERVICE (no patching) ----------
    service = CFSSubscriberService(mqtt_config, cfs_config)
    service.start()
    print("\n[DEBUG] Service started, waiting for messages...")

    # ---------- WAIT ----------
    timeout = 300
    start = time.time()
    new_files = []

    while time.time() - start < timeout:
        current_files = set(os.listdir(test_dir))
        new_files = [f for f in current_files - files_before if not f.endswith(".jsonl")]
        if new_files:
            print(f"\n[DEBUG] New files found: {new_files}")
            break
        time.sleep(0.5)

    service.stop()

    print(f"\n[DEBUG] Files after test: {set(os.listdir(test_dir))}")
    assert len(new_files) > 0, "No new file received from live MQTT stream"