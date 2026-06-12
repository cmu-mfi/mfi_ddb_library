import os
import json
import shutil
import sys
import pytest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))


from connector.connector import CFSSubscriberService

# -------- MOCK DATA --------
MOCK_TOPIC = "test/topic"
MOCK_FILE_CONTENT = b"1234567890"
MOCK_TRIAL_ID = "trial_1"
MOCK_TIMESTAMP = "2026-01-01T00-00-00"

VALID_DATA = {
    "file_name": "test.jpg",
    "file_type": "jpg",
    "size": 10,
    "timestamp": MOCK_TIMESTAMP,
    "trial_id": MOCK_TRIAL_ID,
    "file": MOCK_FILE_CONTENT
}

VALID_ATTRIBUTES = {
    "description": {"key": "value"},
    "trial_id": MOCK_TRIAL_ID,
    "timestamp": MOCK_TIMESTAMP
}


# -------- FIXTURES --------

@pytest.fixture
def service(tmp_path):
    """CFSSubscriberService in test mode (no MQTT)"""
    return CFSSubscriberService({}, {"save_directory": str(tmp_path), "topic": MOCK_TOPIC})


@pytest.fixture
def clean_test_dir():
    """Fixture for tests that need a real directory path (not tmp_path)"""
    test_dir = "./test_output"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    yield test_dir
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)


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


# -------- UNIT TESTS --------

class TestSaveBlob:
    def test_creates_blob_file(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        files = print_dir(tmp_path)
        assert any(f.endswith(".jpg") for f in files)

    def test_creates_metadata_file(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        files = print_dir(tmp_path)
        json_files = [f for f in files if f.endswith(".json")]
        print(f"\n  Metadata files: {json_files}")
        assert len(json_files) > 0

    def test_creates_index(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        files = print_dir(tmp_path)
        print_index(tmp_path)
        assert "index.jsonl" in files

    def test_index_record_contents(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        print_index(tmp_path)
        with open(os.path.join(tmp_path, "index.jsonl")) as f:
            record = json.loads(f.readline())
        print(f"\n  Index record: {record}")
        assert record["topic"] == MOCK_TOPIC
        assert record["timestamp"] == MOCK_TIMESTAMP
        assert record["trial_id"] == MOCK_TRIAL_ID
        assert record["file_type"] == "jpg"
        assert "file_id" in record

    def test_blob_file_contents(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        files = [f for f in os.listdir(tmp_path) if f.endswith(".jpg")]
        print(f"\n  Blob file: {files[0]}")
        with open(os.path.join(tmp_path, files[0]), "rb") as f:
            contents = f.read()
        print(f"  Contents: {contents}")
        assert contents == MOCK_FILE_CONTENT

    def test_exception_during_write_returns_none(self, service, tmp_path, monkeypatch):
        def bad_open(*args, **kwargs):
            raise IOError("fail")
        monkeypatch.setattr("builtins.open", bad_open)
        result = service.storage.save_blob(MOCK_TOPIC, VALID_DATA.copy())
        print(f"\n  Result: {result}")
        assert result is None

    def test_multiple_blobs_create_multiple_entries(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        print_dir(tmp_path)
        print_index(tmp_path)
        with open(os.path.join(tmp_path, "index.jsonl")) as f:
            records = [json.loads(line) for line in f]
        print(f"\n  Records: {records}")
        assert len(records) == 2
        # file_ids should be unique
        assert records[0]["file_id"] != records[1]["file_id"]


class TestHandleData:
    def test_valid_data_saves_files(self, service, tmp_path):
        service._handle_data(MOCK_TOPIC, VALID_DATA.copy())
        files = print_dir(tmp_path)
        assert len(files) >= 3  # blob + metadata + index

    def test_missing_keys_saves_nothing(self, service, tmp_path):
        data = {"file": b"123"}  # missing required keys
        service._handle_data(MOCK_TOPIC, data)
        files = print_dir(tmp_path)
        print(f"\n  Files after missing keys: {files}")
        assert len(files) == 0

    def test_invalid_file_type_saves_nothing(self, service, tmp_path):
        data = {**VALID_DATA, "file": "not_bytes"}
        service._handle_data(MOCK_TOPIC, data)
        files = print_dir(tmp_path)
        print(f"\n  Files after invalid file type: {files}")
        assert len(files) == 0

    def test_missing_single_key_saves_nothing(self, service, tmp_path):
        for key in ["file_name", "file_type", "size", "timestamp", "file"]:
            data = {k: v for k, v in VALID_DATA.items() if k != key}
            service._handle_data(MOCK_TOPIC, data)
            files = list(os.listdir(tmp_path))
            print(f"\n  Missing key '{key}', files: {files}")
            assert len(files) == 0


class TestHandleAttributes:
    def test_valid_attributes_saves_file(self, service, tmp_path):
        service._handle_attributes(MOCK_TOPIC, VALID_ATTRIBUTES.copy())
        files = list(tmp_path.iterdir())
        print(f"\n  Files: {[f.name for f in files]}")
        assert any(f.suffix == ".json" for f in files)

    def test_description_gets_topic_injected(self, service, tmp_path):
        service._handle_attributes(MOCK_TOPIC, VALID_ATTRIBUTES.copy())
        files = [f for f in tmp_path.iterdir() if f.suffix == ".json"]
        with open(files[0]) as f:
            saved = json.load(f)
        print(f"\n  Saved attributes: {saved}")
        assert saved["topic"] == MOCK_TOPIC

    def test_missing_description_saves_nothing(self, service, tmp_path):
        data = {"trial_id": MOCK_TRIAL_ID, "timestamp": MOCK_TIMESTAMP}
        service._handle_attributes(MOCK_TOPIC, data)
        files = list(tmp_path.iterdir())
        print(f"\n  Files after missing description: {files}")
        assert len(files) == 0


class TestCallback:
    def test_unknown_data_type_saves_nothing(self, service, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "connector.connector.BlobTopicFamily.process_message",
            lambda payload: ("unknown", {})
        )
        service._callback(MOCK_TOPIC, b"dummy")
        files = list(tmp_path.iterdir())
        print(f"\n  Files after unknown type: {files}")
        assert len(files) == 0

    def test_parse_failure_saves_nothing(self, service, tmp_path, monkeypatch):
        def bad_parse(payload):
            raise ValueError("bad payload")
        monkeypatch.setattr(
            "connector.connector.BlobTopicFamily.process_message",
            bad_parse
        )
        service._callback(MOCK_TOPIC, b"dummy")
        files = list(tmp_path.iterdir())
        print(f"\n  Files after parse failure: {files}")
        assert len(files) == 0

    def test_data_callback_saves_files(self, service, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "connector.connector.BlobTopicFamily.process_message",
            lambda payload: ("data", VALID_DATA.copy())
        )
        service._callback(MOCK_TOPIC, b"dummy")
        files = print_dir(tmp_path)
        assert len(files) >= 3

    def test_attributes_callback_saves_file(self, service, tmp_path, monkeypatch):
        monkeypatch.setattr(
            "connector.connector.BlobTopicFamily.process_message",
            lambda payload: ("attributes", VALID_ATTRIBUTES.copy())
        )
        service._callback(MOCK_TOPIC, b"dummy")
        files = list(tmp_path.iterdir())
        print(f"\n  Files after attributes callback: {[f.name for f in files]}")
        assert any(f.suffix == ".json" for f in files)