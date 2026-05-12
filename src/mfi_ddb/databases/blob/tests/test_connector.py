import os
import shutil

from mfi_ddb.databases.blob.connector import CFSSubscriberService


def test_save_blob_creates_files():
    test_dir = "./test_output"

    # clean before test
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)

    cfs_config = {
        "save_directory": test_dir,
        "topic": "test/topic"
    }

    service = CFSSubscriberService({}, cfs_config)

    test_data = {
        "file_name": "test.jpg",
        "file_type": "jpg",
        "size": 10,
        "timestamp": "2026-01-01T00:00:00",
        "trial_id": "trial_1",
        "file": b"1234567890"
    }

    service._handle_data("test/topic", test_data)

    # -------- ASSERTIONS --------
    assert os.path.exists(test_dir)

    files = os.listdir(test_dir)

    assert len(files) >= 3
    assert "index.jsonl" in files
    assert any(f.endswith(".json") for f in files)
    assert any(f.endswith(".jpg") for f in files)


def test_handle_attributes(tmp_path):
    service = CFSSubscriberService({}, {"save_directory": str(tmp_path)})

    topic = "test/topic"
    data = {
        "description": {"key": "value"},
        "trial_id": "t1",
        "timestamp": "123"
    }

    service._handle_attributes(topic, data)

    files = list(tmp_path.iterdir())
    assert any(f.suffix == ".json" for f in files)


def test_handle_attributes_missing_description(tmp_path):
    service = CFSSubscriberService({}, {"save_directory": str(tmp_path)})

    topic = "test/topic"
    data = {
        "trial_id": "t1",
        "timestamp": "123"
    }

    service._handle_attributes(topic, data)

    assert len(list(tmp_path.iterdir())) == 0


def test_handle_data_missing_keys(tmp_path):
    service = CFSSubscriberService({}, {"save_directory": str(tmp_path)})

    topic = "test/topic"
    data = {"file": b"123"}  # missing required keys

    service._handle_data(topic, data)

    assert len(list(tmp_path.iterdir())) == 0


def test_handle_data_invalid_file(tmp_path):
    service = CFSSubscriberService({}, {"save_directory": str(tmp_path)})

    topic = "test/topic"
    data = {
        "file_name": "x",
        "file_type": "jpg",
        "size": 1,
        "timestamp": "123",
        "file": "not_bytes"
    }

    service._handle_data(topic, data)

    assert len(list(tmp_path.iterdir())) == 0


def test_callback_unknown_data_type(tmp_path, monkeypatch):
    service = CFSSubscriberService({}, {"save_directory": str(tmp_path)})

    class FakeMessage:
        topic = "test/topic"
        payload = b"dummy"

    # force unknown data_type
    monkeypatch.setattr(
        "mfi_ddb.databases.blob.connector.BlobTopicFamily.process_message",
        lambda payload: ("unknown", {})
    )

    service._callback(FakeMessage())

    assert len(list(tmp_path.iterdir())) == 0


def test_save_blob_exception(tmp_path, monkeypatch):
    service = CFSSubscriberService({}, {"save_directory": str(tmp_path)})

    topic = "test/topic"
    data = {
        "file_name": "x",
        "file_type": "jpg",
        "size": 1,
        "timestamp": "123",
        "file": b"123"
    }

    # force exception during file write
    def bad_open(*args, **kwargs):
        raise IOError("fail")

    monkeypatch.setattr("builtins.open", bad_open)

    result = service.storage.save_blob(topic, data)

    assert result is None