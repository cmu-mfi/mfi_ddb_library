import json
import os
import sys
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws.blobapi import BlobAPI

# pytest -v -s test_blobapi_integration.py

# -------- CONFIG --------
TEST_OUTPUT_DIR = os.path.join("test_output")
INDEX_PATH = os.path.join(TEST_OUTPUT_DIR, "index.jsonl")

pytestmark = pytest.mark.skipif(
    not os.path.exists(INDEX_PATH),
    reason="No real index.jsonl found at test_output/index.jsonl, skipping integration tests"
)


# -------- DEBUG HELPERS --------
def print_blob(blob):
    print(f"  file_id={blob.file_id} | topic={blob.topic} | timestamp={blob.timestamp} | size={len(blob.file)} bytes")

def print_result(result):
    print(f"  count={len(result['data'])}")
    for blob in result["data"]:
        print_blob(blob)
    print(f"  nextPageToken={result['nextPageToken']}")


# -------- FIXTURES --------

@pytest.fixture
def api():
    return BlobAPI(blob_dir=TEST_OUTPUT_DIR, index_path=INDEX_PATH)

@pytest.fixture
def first_record():
    """Load the first record from the real index.jsonl"""
    with open(INDEX_PATH, "r") as f:
        record = json.loads(f.readline())
    print(f"\nFirst record from index: {record}")
    return record

@pytest.fixture
def wildcard_topic(first_record):
    topic_parts = first_record["topic"].split("/")
    wildcard = "/".join(topic_parts[:-1]) + "/#"
    print(f"\nDerived wildcard topic: {wildcard}")
    return wildcard


# -------- INTEGRATION TESTS --------

def test_real_index_exists():
    assert os.path.exists(INDEX_PATH), f"index.jsonl not found at {INDEX_PATH}"
    print(f"\nIndex found at: {INDEX_PATH}")


def test_get_data_point(api, first_record):
    result = api.get_data_point(
        topic=first_record["topic"],
        timestamp=first_record["timestamp"]
    )
    print("\n--- get_data_point result ---")
    print_blob(result)
    assert result is not None
    assert result.topic == first_record["topic"]
    assert result.file_id == first_record["file_id"]
    assert len(result.file) > 0


def test_get_data_point_download(api, first_record):
    result = api.get_data_point(
        topic=first_record["topic"],
        timestamp=first_record["timestamp"]
    )
    print("\n--- get_data_point_download ---")
    print_blob(result)

    file_ext = result.file_type if result.file_type.startswith(".") else f".{result.file_type}"
    output_path = os.path.join(
        os.path.dirname(__file__),
        f"download_{result.file_id}{file_ext}"
    )
    with open(output_path, "wb") as f:
        f.write(result.file)

    print(f"File written to: {output_path}")
    assert os.path.exists(output_path)
    assert len(result.file) > 0


def test_get_data_range(api, first_record):
    result = api.get_data_range(
        topic=first_record["topic"],
        start_time="2000-01-01T00:00:00Z",
        end_time="2100-01-01T00:00:00Z"
    )
    print(f"\n--- get_data_range: topic={first_record['topic']} ---")
    print_result(result)
    assert len(result["data"]) > 0


def test_get_data_range_wildcard(api, wildcard_topic):
    result = api.get_data_range(
        topic=wildcard_topic,
        start_time="2000-01-01T00:00:00Z",
        end_time="2100-01-01T00:00:00Z"
    )
    print(f"\n--- get_data_range wildcard: topic={wildcard_topic} ---")
    print_result(result)
    assert len(result["data"]) > 0


def test_get_data_range_pagination(api, first_record):
    """Verify pagination works end-to-end on real data"""
    page_size = 1
    all_ids = []
    page_token = None
    page_num = 0

    while True:
        result = api.get_data_range(
            topic=first_record["topic"],
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z",
            page_size=page_size,
            page_token=page_token
        )
        page_num += 1
        print(f"\n--- Page {page_num} ---")
        print_result(result)

        page_ids = [b.file_id for b in result["data"]]
        # No duplicates within or across pages
        assert len(set(page_ids) & set(all_ids)) == 0, f"Duplicate file_ids found on page {page_num}"
        all_ids.extend(page_ids)

        page_token = result["nextPageToken"]
        if not page_token:
            break

    print(f"\nTotal blobs across all pages: {len(all_ids)}")
    assert len(all_ids) > 0


def test_iter_matching_records_exact(api, first_record):
    records = list(api._iter_matching_records(first_record["topic"]))
    print(f"\n--- _iter_matching_records exact: topic={first_record['topic']} ---")
    print(f"  count={len(records)}")
    for r in records:
        print(f"  file_id={r['file_id']} | topic={r['topic']} | timestamp={r['timestamp']}")
    assert len(records) > 0
    for r in records:
        assert r["topic"] == first_record["topic"]


def test_iter_matching_records_wildcard(api, wildcard_topic, first_record):
    records = list(api._iter_matching_records(wildcard_topic))
    print(f"\n--- _iter_matching_records wildcard: topic={wildcard_topic} ---")
    print(f"  count={len(records)}")
    for r in records:
        print(f"  file_id={r['file_id']} | topic={r['topic']} | timestamp={r['timestamp']}")
    assert len(records) > 0
    prefix = wildcard_topic[:-2]
    for r in records:
        assert r["topic"].startswith(prefix)
    assert any(
        r["topic"] == first_record["topic"] and
        r["timestamp"] == first_record["timestamp"]
        for r in records
    )