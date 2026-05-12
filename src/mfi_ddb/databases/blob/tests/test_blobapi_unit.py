import json
import os
import pytest
from mfi_ddb.databases.blob.blobapi import BlobAPI

#pytest -v -s test_blobapi_unit.py

# -------- CONFIG --------
TEST_OUTPUT_DIR = os.path.join("test_output")
INDEX_PATH = os.path.join(TEST_OUTPUT_DIR, "index.jsonl")


@pytest.fixture
def api():
    return BlobAPI(
        blob_dir=TEST_OUTPUT_DIR,
        index_path=INDEX_PATH
    )

@pytest.fixture
def first_record():
    """Load the first record from the real index.jsonl"""
    with open(INDEX_PATH, "r") as f:
        return json.loads(f.readline())


# -------- INTEGRATION TESTS --------

def test_real_index_exists():
    assert os.path.exists(INDEX_PATH), f"index.jsonl not found at {INDEX_PATH}"

def test_real_get_data_point(api, first_record):
    result = api.get_data_point(
        topic=first_record["topic"],
        timestamp=first_record["timestamp"]
    )
    print(f"\nfile_id: {result.file_id}")
    print(f"topic: {result.topic}")
    print(f"timestamp: {result.timestamp}")
    print(f"file_type: {result.file_type}")
    print(f"file size: {len(result.file)} bytes")
    assert result is not None

def test_real_get_data_range(api, first_record):
    result = api.get_data_range(
        topic=first_record["topic"],
        start_time="2000-01-01T00:00:00Z",
        end_time="2100-01-01T00:00:00Z"
    )
    print(f"\nTotal blobs returned: {len(result['data'])}")
    for blob in result["data"]:
        print(f"  - {blob.file_id} | {blob.timestamp} | {len(blob.file)} bytes")
    print(f"nextPageToken: '{result['nextPageToken']}'")
    assert len(result["data"]) > 0

def test_get_data_point_download(api, first_record):
    result = api.get_data_point(
        topic=first_record["topic"],
        timestamp=first_record["timestamp"]
    )
    
    # write the file to disk so you can inspect it
    file_ext = result.file_type if result.file_type.startswith(".") else f".{result.file_type}"
    output_path = os.path.join(os.path.dirname(__file__), f"download_{result.file_id}{file_ext}")
    
    with open(output_path, "wb") as f:
        f.write(result.file)
    
    print(f"\nFile written to: {output_path}")
    print(f"File size: {len(result.file)} bytes")
    
    assert result is not None
    assert os.path.exists(output_path)
    assert len(result.file) > 0
