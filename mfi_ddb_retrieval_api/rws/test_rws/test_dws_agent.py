import pytest
from unittest.mock import patch
from google.protobuf.timestamp_pb2 import Timestamp
from app.utils.dws.gen.models_pb2 import Datapoint
from app.utils.dws.gen.service_pb2 import GetDataRangeResponse


# ---------------------------------------------------------------------------
# Config: 3 topic families, one dedicated service each.
#
# Topic family is always the FIRST segment before the first '/'.
#   "kv/sensor/temp"      → family "kv"
#   "mqtt/device/status"  → family "mqtt"
#   "stream/video/cam1"   → family "stream"
#
# The same sub-path (e.g. "sensor/temp") can appear under multiple families;
# routing is decided purely by that first segment.
# ---------------------------------------------------------------------------

MULTI_SERVICE_CONFIG = {
    "kv":           [{"name": "service_kv",     "url": "localhost:50051"}],
    "historian":    [{"name": "service_time",   "url": "localhost:50052"},
                     {"name": "service_hist",   "url": "localhost:50030"}],
    "blob":         [{"name": "service_blob",   "url": "localhost:50053"}]
}

TIME_START = "2024-01-01T00:00:00"
TIME_END   = "2024-01-01T01:00:00"

# Timestamp that falls within [TIME_START, TIME_END]
TS_IN_RANGE = 1_704_067_200  # 2024-01-01T00:00:00 UTC exactly


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_real_datapoint(
    topic: str,
    seconds: int,
    int_value: int = 0,
    float_value: float = 0.0,
    string_value: str = "",
) -> Datapoint:
    """Build a real protobuf Datapoint — not a MagicMock."""
    dp = Datapoint()
    dp.topic = topic
    dp.timestamp.CopyFrom(Timestamp(seconds=seconds))
    if int_value!=0:
        dp.int_value = int_value
    elif float_value!=0.0:
        dp.float_value = float_value
    elif string_value!="":
        dp.string_value = string_value
    return dp


def _make_real_response(*datapoints: Datapoint, next_page_token: str = "") -> GetDataRangeResponse:
    """Build a real protobuf GetDataRangeResponse."""
    response = GetDataRangeResponse()
    response.datapoints.extend(datapoints)
    response.next_page_token = next_page_token
    return response


def _make_file_datapoint(
    topic: str,
    seconds: int,
    filename: str,
    content: bytes,
    sha256: str = "",
) -> Datapoint:
    """Build a real protobuf Datapoint carrying a file_value."""
    dp = Datapoint()
    dp.topic = topic
    dp.timestamp.CopyFrom(Timestamp(seconds=seconds))
    dp.file_value.filename = filename
    dp.file_value.content = content
    if sha256:
        dp.file_value.sha256 = sha256
    return dp


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def agent():
    """Agent pre-wired with 3 topic families, bypassing YAML loading."""
    from app.services.dws_agent import __DwsAgent

    instance = __DwsAgent.__new__(__DwsAgent)
    instance.config = MULTI_SERVICE_CONFIG
    return instance


# ===========================================================================
# Test 1 – Invalid topic family → empty result, no server call made
# ===========================================================================

def test_invalid_topic_family_returns_empty(agent):
    with patch.object(agent, "_call_dws_server") as mock_call:
        result = agent.get_data("unknown/sensor/temp", TIME_START, TIME_END)

    assert result == {}, \
        "An unrecognised topic family should return an empty dict."
    assert mock_call.call_count == 0, \
        "No server call should be made for an unknown topic family."


# ===========================================================================
# Test 2 – Time out of bounds → topic absent from result
# ===========================================================================

def test_out_of_range_time_returns_empty(agent):
    import datetime

    # Server returns nothing — no datapoints fall in [TIME_START, TIME_END]
    with patch.object(agent, "_call_dws_server", return_value=_make_real_response()) as mock_call:
        result = agent.get_data("kv/sensor/temp", TIME_START, TIME_END)

    assert "kv/sensor/temp" not in result, \
        "A topic with no in-range datapoints should not appear in the result."

    # Verify the correct time bounds were forwarded in the real request
    actual_request = mock_call.call_args[0][1]  # (endpoint, request)
    assert actual_request.start_time.seconds == int(
        datetime.datetime.fromisoformat(TIME_START).timestamp()
    )
    assert actual_request.end_time.seconds == int(
        datetime.datetime.fromisoformat(TIME_END).timestamp()
    )


# ===========================================================================
# Test 3 – Single datapoint returned correctly
# ===========================================================================

def test_single_datapoint_returned_correctly(agent):
    import datetime

    dp = _make_real_datapoint("kv/sensor/temp", TS_IN_RANGE, float_value=23.5)

    with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp)):
        result = agent.get_data("kv/sensor/temp", TIME_START, TIME_END)

    assert "kv/sensor/temp" in result
    assert len(result["kv/sensor/temp"]) == 1

    entry = result["kv/sensor/temp"][0]
    print(dp)
    assert entry["value"] == pytest.approx(23.5)
    assert entry["timestamp"] == datetime.datetime.fromtimestamp(TS_IN_RANGE).isoformat()

# ===========================================================================
# Test 4 – MQTT wildcard "kv/sensor/#" is forwarded as-is to the server.
#          Result is keyed by the concrete topic names in the returned
#          datapoints, not by the wildcard string itself.
# ===========================================================================

def test_mqtt_wildcard_returns_matching_topics(agent):
    dp_temp     = _make_real_datapoint("kv/sensor/temp",     TS_IN_RANGE, float_value=23.5)
    dp_humidity = _make_real_datapoint("kv/sensor/humidity", TS_IN_RANGE, float_value=58.0)

    with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp_temp, dp_humidity)) as mock_call:
        result = agent.get_data("kv/sensor/#", TIME_START, TIME_END)

    actual_request = mock_call.call_args[0][1]  # (endpoint, request)
    assert actual_request.topic == "kv/sensor/#"

    assert "kv/sensor/temp"     in result
    assert "kv/sensor/humidity" in result
    assert "kv/sensor/#"        not in result, \
        "The wildcard string itself must not appear as a result key."

    assert result["kv/sensor/temp"][0]["value"]     == pytest.approx(23.5)
    assert result["kv/sensor/humidity"][0]["value"] == pytest.approx(58.0)


# ===========================================================================
# Test 5 – Pagination: all pages are fetched and merged into the result
#
# Page sequence:
#   initial call  → page_token=""      → 2 datapoints, next_page_token="tok_1"
#   second call   → page_token="tok_1" → 2 datapoints, next_page_token="tok_2"
#   third call    → page_token="tok_2" → 1 datapoint,  next_page_token=""
#
# ===========================================================================

def test_pagination_all_pages_fetched_and_merged(agent):
    dp1 = _make_real_datapoint("kv/sensor/temp", TS_IN_RANGE + 0, float_value=10.0)
    dp2 = _make_real_datapoint("kv/sensor/temp", TS_IN_RANGE + 1, float_value=11.0)
    dp3 = _make_real_datapoint("kv/sensor/temp", TS_IN_RANGE + 2, float_value=12.0)
    dp4 = _make_real_datapoint("kv/sensor/temp", TS_IN_RANGE + 3, float_value=13.0)
    dp5 = _make_real_datapoint("kv/sensor/temp", TS_IN_RANGE + 4, float_value=14.0)

    page1 = _make_real_response(dp1, dp2, next_page_token="tok_1")
    page2 = _make_real_response(dp3, dp4, next_page_token="tok_2")
    page3 = _make_real_response(dp5,      next_page_token="")

    with patch.object(agent, "_call_dws_server", side_effect=[page1, page2, page3]) as mock_call:
        result = agent.get_data("kv/sensor/temp", TIME_START, TIME_END)

    # Three calls: one initial + two paginated
    assert mock_call.call_count == 3

    # page_token must chain correctly across calls: (endpoint, request) per call
    call1_req = mock_call.call_args_list[0][0][1]
    call2_req = mock_call.call_args_list[1][0][1]
    call3_req = mock_call.call_args_list[2][0][1]

    assert call1_req.page_token == "",      "Initial call must have an empty page_token."
    assert call2_req.page_token == "tok_1", "Second call must carry the first page token."
    assert call3_req.page_token == "tok_2", "Third call must carry the second page token."

    # All 5 datapoints across all 3 pages must be merged into a single list
    assert "kv/sensor/temp" in result
    assert len(result["kv/sensor/temp"]) == 5

    values = [entry["value"] for entry in result["kv/sensor/temp"]]
    assert values == pytest.approx([10.0, 11.0, 12.0, 13.0, 14.0])


# ===========================================================================
# Test 6 – file_value: bytes=True saves the content to a temporary file and
#          returns the filepath as value, plus the original filename under
#          the "filename" key.
# ===========================================================================

def test_file_value_saved_to_temp_file(agent):
    import datetime
    import hashlib
    import shutil
    from pathlib import Path

    content = b"hello world"
    sha256 = hashlib.sha256(content).hexdigest()
    dp = _make_file_datapoint("blob/files/data", TS_IN_RANGE, "report.pdf", content, sha256)

    with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp)):
        result = agent.get_data("blob/files/data", TIME_START, TIME_END, bytes=True)

    entry = result["blob/files/data"][0]
    filepath = Path(entry["value"])
    try:
        assert filepath.is_file(), "The file content must be saved to disk."
        assert filepath.read_bytes() == content, \
            "The saved file must contain the exact bytes sent by the DWS server."
        assert filepath.name == "0000_report.pdf", \
            "On-disk names are prefixed with an index to avoid collisions."
        assert entry["filename"] == "report.pdf", \
            "The original filename must be returned under the 'filename' key."
        assert entry["timestamp"] == datetime.datetime.fromtimestamp(TS_IN_RANGE).isoformat()
    finally:
        shutil.rmtree(filepath.parent, ignore_errors=True)


# ===========================================================================
# Test 7 – file_value: a wrong sha256 is logged as a warning, but the file
#          is still saved and returned.
# ===========================================================================

def test_file_value_sha256_mismatch_logs_warning(agent):
    import shutil
    from pathlib import Path

    dp = _make_file_datapoint("blob/files/data", TS_IN_RANGE, "report.pdf", b"hello world", sha256="deadbeef")

    with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp)):
        result = agent.get_data("blob/files/data", TIME_START, TIME_END, bytes=True)

    entry = result["blob/files/data"][0]
    filepath = Path(entry["value"])
    try:
        assert filepath.is_file()
        assert filepath.read_bytes() == b"hello world"
    finally:
        shutil.rmtree(filepath.parent, ignore_errors=True)


def test_file_value_sha256_match_does_not_warn(agent, caplog):
    import hashlib
    import shutil
    from pathlib import Path

    content = b"hello world"
    sha256 = hashlib.sha256(content).hexdigest()
    dp = _make_file_datapoint("blob/files/data", TS_IN_RANGE, "report.pdf", content, sha256)

    with caplog.at_level("WARNING"):
        with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp)):
            result = agent.get_data("blob/files/data", TIME_START, TIME_END, bytes=True)

    entry = result["blob/files/data"][0]
    try:
        assert "SHA256 mismatch" not in caplog.text
        assert Path(entry["value"]).read_bytes() == content
    finally:
        shutil.rmtree(Path(entry["value"]).parent, ignore_errors=True)


# ===========================================================================
# Test 8 – file_value: bytes=False (default) returns only the filename,
#          no files are written to disk and no "filename" key is added.
# ===========================================================================

def test_file_value_filename_only_when_bytes_false(agent):
    dp = _make_file_datapoint("blob/files/data", TS_IN_RANGE, "report.pdf", b"hello world")

    with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp)):
        result = agent.get_data("blob/files/data", TIME_START, TIME_END)

    entry = result["blob/files/data"][0]
    assert entry["value"] == "report.pdf", \
        "With bytes=False the value must be the original filename, not a filepath."
    assert "filename" not in entry


# ===========================================================================
# Test 9 – multiple file_values with the same filename: all are saved with
#          distinct filepaths in the same temporary directory.
# ===========================================================================

def test_multiple_file_values_same_name_distinct_paths(agent):
    import shutil
    from pathlib import Path

    dp1 = _make_file_datapoint("blob/files/a", TS_IN_RANGE, "same.bin", b"AAA")
    dp2 = _make_file_datapoint("blob/files/b", TS_IN_RANGE + 1, "same.bin", b"BBB")

    with patch.object(agent, "_call_dws_server", return_value=_make_real_response(dp1, dp2)):
        result = agent.get_data("blob/files/a", TIME_START, TIME_END, bytes=True)

    path_a = Path(result["blob/files/a"][0]["value"])
    path_b = Path(result["blob/files/b"][0]["value"])
    try:
        assert path_a != path_b, \
            "Same-named files must not overwrite each other."
        assert path_a.parent == path_b.parent, \
            "All files of one call must live in the same temporary directory."
        assert path_a.name == "0000_same.bin"
        assert path_b.name == "0001_same.bin"
        assert path_a.read_bytes() == b"AAA"
        assert path_b.read_bytes() == b"BBB"
    finally:
        shutil.rmtree(path_a.parent, ignore_errors=True)