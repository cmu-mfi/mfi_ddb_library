import sys
import os
import time
import json
import logging
import pytest
import yaml
from pathlib import Path

TEST_DIR = Path(__file__).resolve().parent        # tests/
CONNECTOR_DIR = TEST_DIR.parent / "connector"     # connector/
CONFIG_PATH = str(CONNECTOR_DIR / "config.yaml")  # connector/config.yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from connector.connector import CFSSubscriberService

# pytest -v -s test_connector_wmqtt.py

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger("test_mqtt_integration")

# -------- CONFIG --------
TEST_OUTPUT_DIR = str(TEST_DIR / "test_output")

# -------- SKIP IF NO CONFIG --------
pytestmark = pytest.mark.skipif(
    not os.path.exists(CONFIG_PATH),
    reason=f"No config found at {CONFIG_PATH}, skipping live integration tests"
)


# -------- FIXTURES --------

@pytest.fixture
def mqtt_config():
    print(f"\n  Loading config from: {CONFIG_PATH}")
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

@pytest.fixture
def cfs_config():
    with open(CONFIG_PATH) as f:
        config = yaml.safe_load(f)
    # Override save directory for test
    config["save_directory"] = TEST_OUTPUT_DIR
    print(f"\n  Save directory: {TEST_OUTPUT_DIR}")
    return config

@pytest.fixture
def test_output_dir():
    """Create test output dir, leave files after test for inspection"""
    os.makedirs(TEST_OUTPUT_DIR, exist_ok=True)
    return TEST_OUTPUT_DIR


# -------- DEBUG HELPERS --------

def print_dir(path):
    files = list(os.listdir(path))
    print(f"\n  Directory contents ({path}):")
    for f in files:
        print(f"    {f}")
    return files

def print_index(path):
    index_path = os.path.join(path, "index.jsonl")
    if os.path.exists(index_path):
        print(f"\n  Index contents:")
        with open(index_path) as f:
            for line in f:
                print(f"    {line.strip()}")


# -------- INTEGRATION TESTS --------

def test_mqtt_connection_and_ingestion(mqtt_config, cfs_config, test_output_dir):
    """
    Live test: connects to real MQTT broker and waits for a blob message.
    Verifies that at least one blob file, metadata file, and index entry are created.
    Timeout: 300 seconds.
    """
    files_before = set(os.listdir(test_output_dir))
    print(f"\n  Files before test: {files_before}")

    service = CFSSubscriberService(mqtt_config, cfs_config)

    try:
        service.start()
        assert service.running, "Service failed to start"
        print("\n  Service started, waiting for messages...")

        # -------- WAIT FOR NEW FILES --------
        timeout = 300
        poll_interval = 0.5
        start = time.time()
        new_blob_files = []

        while time.time() - start < timeout:
            current_files = set(os.listdir(test_output_dir))
            new_files = current_files - files_before
            new_blob_files = [
                f for f in new_files
                if not f.endswith(".jsonl") and not f.endswith(".json")
            ]
            if new_blob_files:
                elapsed = time.time() - start
                print(f"\n  Message received after {elapsed:.1f}s")
                print(f"  New blob files: {new_blob_files}")
                break
            time.sleep(poll_interval)

    finally:
        service.stop()
        print("\n  Service stopped")

    # -------- REPORT FINAL STATE --------
    print_dir(test_output_dir)
    print_index(test_output_dir)

    # -------- ASSERTIONS --------
    assert service.running is False, "Service should be stopped"
    assert len(new_blob_files) > 0, f"No blob files received within {timeout}s"

    # Verify index was updated
    index_path = os.path.join(test_output_dir, "index.jsonl")
    assert os.path.exists(index_path), "index.jsonl not created"

    with open(index_path) as f:
        records = [json.loads(line) for line in f]

    new_records = [r for r in records if r.get("file_id") in [
        os.path.splitext(f)[0] for f in new_blob_files
    ]]

    print(f"\n  New index records: {new_records}")
    assert len(new_records) > 0, "No index records found for new blob files"

    for record in new_records:
        print(f"\n  Validating record: {record}")
        assert "file_id" in record
        assert "topic" in record
        assert "timestamp" in record
        assert "file_type" in record


def test_mqtt_service_stops_cleanly(mqtt_config, cfs_config):
    """Verify service starts and stops without errors"""
    service = CFSSubscriberService(mqtt_config, cfs_config)
    service.start()
    assert service.running is True
    print("\n  Service running — stopping now")
    service.stop()
    assert service.running is False
    print("  Service stopped cleanly")