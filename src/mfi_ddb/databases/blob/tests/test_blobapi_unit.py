import json
import os
import sys
import pytest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws.blobapi import BlobAPI

# pytest -v -s test_blobapi_unit.py

# -------- CONFIG --------
TEST_OUTPUT_DIR = os.path.join("test_output")
INDEX_PATH = os.path.join(TEST_OUTPUT_DIR, "index.jsonl")

# -------- MOCK DATA --------
MOCK_TOPIC = "mfi-v1.0-blob/CMU/filesystem/local_files_system/data"
MOCK_TOPIC_2 = "mfi-v1.0-blob/CMU/filesystem/local_files_system/other"
MOCK_WILDCARD = "mfi-v1.0-blob/CMU/filesystem/local_files_system/#"
MOCK_TIMESTAMP = "2024-01-01T00:00:00Z"
MOCK_FILE_CONTENT = b"hello world"

MOCK_RECORDS = [
    {"file_id": "abc123", "topic": MOCK_TOPIC,   "timestamp": "1704067200.0", "file_type": ".txt"},  # 2024-01-01T00:00:00Z
    {"file_id": "abc124", "topic": MOCK_TOPIC,   "timestamp": "1704067260.0", "file_type": ".txt"},  # 2024-01-01T00:01:00Z
    {"file_id": "abc125", "topic": MOCK_TOPIC_2, "timestamp": "1704067320.0", "file_type": ".txt"},  # 2024-01-01T00:02:00Z
]


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
def mock_api(tmp_path):
    """BlobAPI instance with synthetic index and blob files — no real files needed"""
    index_path = tmp_path / "index.jsonl"
    index_path.write_text("\n".join(json.dumps(r) for r in MOCK_RECORDS))

    for record in MOCK_RECORDS:
        blob_path = tmp_path / f"{record['file_id']}{record['file_type']}"
        blob_path.write_bytes(MOCK_FILE_CONTENT)

    return BlobAPI(blob_dir=str(tmp_path), index_path=str(index_path))


# -------- UNIT TESTS --------

class TestIterMatchingRecords:
    def test_exact_topic_returns_only_matching(self, mock_api):
        records = list(mock_api._iter_matching_records(MOCK_TOPIC))
        print(f"\nRecords returned: {[r['file_id'] for r in records]}")
        assert len(records) == 2
        for r in records:
            assert r["topic"] == MOCK_TOPIC

    def test_wildcard_returns_all_matching_prefix(self, mock_api):
        records = list(mock_api._iter_matching_records(MOCK_WILDCARD))
        print(f"\nRecords returned: {[r['file_id'] for r in records]}")
        assert len(records) == 3
        prefix = MOCK_WILDCARD[:-2]
        for r in records:
            assert r["topic"].startswith(prefix)

    def test_no_match_returns_empty(self, mock_api):
        records = list(mock_api._iter_matching_records("nonexistent/topic"))
        print(f"\nRecords returned: {records}")
        assert records == []


class TestGetDataPoint:
    def test_exact_timestamp_match(self, mock_api):
        result = mock_api.get_data_point(topic=MOCK_TOPIC, timestamp=MOCK_TIMESTAMP)
        print(f"\nResult: ", end="")
        print_blob(result)
        assert result is not None
        assert result.topic == MOCK_TOPIC
        assert result.file_id == "abc123"
        assert result.file == MOCK_FILE_CONTENT

    def test_closest_past(self, mock_api):
        result = mock_api.get_data_point(topic=MOCK_TOPIC, timestamp="2024-01-01T00:00:50Z", closest_past=True)
        print(f"\nResult (closest past to 00:00:50): ", end="")
        print_blob(result)
        assert result is not None
        assert result.file_id == "abc123"

    def test_closest_future(self, mock_api):
        result = mock_api.get_data_point(topic=MOCK_TOPIC, timestamp="2024-01-01T00:00:50Z", closest_past=False)
        print(f"\nResult (closest future to 00:00:50): ", end="")
        print_blob(result)
        assert result is not None
        assert result.file_id == "abc124"

    def test_wildcard_multiple_topics_returns_none(self, mock_api):
        result = mock_api.get_data_point(topic=MOCK_WILDCARD, timestamp=MOCK_TIMESTAMP)
        print(f"\nResult (wildcard multiple topics): {result}")
        assert result is None

    def test_no_records_returns_none(self, mock_api):
        result = mock_api.get_data_point(topic="nonexistent/topic", timestamp=MOCK_TIMESTAMP)
        print(f"\nResult (no records): {result}")
        assert result is None

    def test_missing_timestamp_raises(self, mock_api):
        with pytest.raises(ValueError):
            mock_api.get_data_point(topic=MOCK_TOPIC)

    def test_no_closest_past_candidate_raises(self, mock_api):
        with pytest.raises(FileNotFoundError):
            mock_api.get_data_point(topic=MOCK_TOPIC, timestamp="2000-01-01T00:00:00Z", closest_past=True)

    def test_no_closest_future_candidate_raises(self, mock_api):
        with pytest.raises(FileNotFoundError):
            mock_api.get_data_point(topic=MOCK_TOPIC, timestamp="2100-01-01T00:00:00Z", closest_past=False)


class TestGetDataRange:
    def test_returns_blobs_for_exact_topic(self, mock_api):
        result = mock_api.get_data_range(
            topic=MOCK_TOPIC,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z"
        )
        print("\n--- Exact topic ---")
        print_result(result)
        assert len(result["data"]) == 2
        assert result["nextPageToken"] is None

    def test_wildcard_returns_all_topics(self, mock_api):
        result = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z"
        )
        print("\n--- Wildcard ---")
        print_result(result)
        assert len(result["data"]) == 3

    def test_results_sorted_by_timestamp(self, mock_api):
        result = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z"
        )
        timestamps = [float(b.timestamp) for b in result["data"]]
        print(f"\nTimestamps: {timestamps}")
        assert timestamps == sorted(timestamps)

    def test_pagination_first_page(self, mock_api):
        result = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z",
            page_size=2
        )
        print("\n--- Page 1 ---")
        print_result(result)
        assert len(result["data"]) == 2
        assert result["nextPageToken"] is not None

    def test_pagination_second_page(self, mock_api):
        result = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z",
            page_size=2
        )
        print("\n--- Page 1 ---")
        print_result(result)

        result2 = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z",
            page_size=2,
            page_token=result["nextPageToken"]
        )
        print("--- Page 2 ---")
        print_result(result2)

        assert len(result2["data"]) == 1
        assert result2["nextPageToken"] is None

    def test_pagination_no_duplicates_across_pages(self, mock_api):
        result1 = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z",
            page_size=2
        )
        print("\n--- Page 1 ---")
        print_result(result1)

        result2 = mock_api.get_data_range(
            topic=MOCK_WILDCARD,
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z",
            page_size=2,
            page_token=result1["nextPageToken"]
        )
        print("--- Page 2 ---")
        print_result(result2)

        ids1 = {b.file_id for b in result1["data"]}
        ids2 = {b.file_id for b in result2["data"]}
        print(f"Page 1 ids: {ids1}")
        print(f"Page 2 ids: {ids2}")
        assert ids1.isdisjoint(ids2)

    def test_empty_range_returns_no_data(self, mock_api):
        result = mock_api.get_data_range(
            topic=MOCK_TOPIC,
            start_time="2000-01-01T00:00:00Z",
            end_time="2000-01-02T00:00:00Z"
        )
        print("\n--- Empty range ---")
        print_result(result)
        assert result["data"] == []
        assert result["nextPageToken"] is None

    def test_missing_start_time_raises(self, mock_api):
        with pytest.raises(ValueError):
            mock_api.get_data_range(topic=MOCK_TOPIC, end_time="2100-01-01T00:00:00Z")

    def test_missing_end_time_raises(self, mock_api):
        with pytest.raises(ValueError):
            mock_api.get_data_range(topic=MOCK_TOPIC, start_time="2000-01-01T00:00:00Z")

    def test_no_match_topic_returns_empty(self, mock_api):
        result = mock_api.get_data_range(
            topic="nonexistent/topic",
            start_time="2000-01-01T00:00:00Z",
            end_time="2100-01-01T00:00:00Z"
        )
        print("\n--- No match ---")
        print_result(result)
        assert result["data"] == []
        assert result["nextPageToken"] is None