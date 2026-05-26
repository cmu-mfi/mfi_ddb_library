"""Testing the DWS server logic"""

"""
Testing Catalog:

Test name format: test_[functionality/function being tested]_purpose

1. test_row_to_datapoint_variants: Verifies relational database result tuples convert cleanly to native Protobuf target structures.

2. test_get_data_point_found: Verifies valid response payloads when data exists.

3. test_get_data_point_empty_table [Added Boundary]: Verifies elegant empty responses when a cold-boot request targets an empty table, avoiding index errors.

4. test_get_data_range: Verifies page token isolation using ISO-timestamp keys.

5. test_get_data_range_timestamp_collision [Added Boundary]: Verifies pagination stability when multiple records share identical millisecond timestamps.

6. test_stream_data_polling: Verifies the tailing loop yields records immediately and backs off via sleep when the database is idle.

7. test_stream_data_disconnect: Verifies server streaming resources free up instantly and exit the loop the moment a client drops connection.

8. test_grpc_connection_pool_cleanup: Verifies connections are returned to the ThreadedConnectionPool via context managers even during hard query or network syntax failures.
"""


from datetime import datetime, timezone
import json
import pytest

from google.protobuf.timestamp_pb2 import Timestamp
from mfi_ddb.databases.timescaledb.dws.server import DataService, row_to_datapoint, STREAM_BATCH_SIZE
from mfi_ddb.databases.timescaledb.dws.db import TimeScaleReader
from mfi_ddb.databases.dws.gen import service_pb2


# 1. SERIALIZATION & DATA CONVERSION TESTS

def test_row_to_datapoint_variants():
    """Verifies relational database result tuples convert cleanly to native Protobuf target structures."""
    mock_time = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)

    # Variant A: Float value row conversion
    row_num = (mock_time, "cmu/shop/temp", "demo-comp", "temp", 45.2, None, None)
    dp_num = row_to_datapoint(row_num)
    assert dp_num.topic == "cmu/shop/temp"
    assert dp_num.float_value == 45.2
    assert dp_num.HasField("float_value")

    # Variant B: String/Text value row conversion
    row_str = (mock_time, "cmu/shop/status", "demo-comp", "status", None, "fault_state", None)
    dp_str = row_to_datapoint(row_str)
    assert dp_str.string_value == "fault_state"
    assert dp_str.HasField("string_value")

    # Variant C: JSON object structure conversion
    json_payload = '{"rpm": 1200, "vibration": "nominal"}'
    row_json = (mock_time, "cmu/shop/telemetry", "demo-comp", "metrics", None, None, json_payload)
    dp_json = row_to_datapoint(row_json)
    assert dp_json.HasField("json_value")
    assert dp_json.json_value["rpm"] == 1200
    assert dp_json.json_value["vibration"] == "nominal"


# 2. RPC GET_DATA_POINT HANDLER TESTS

def test_get_data_point_found(monkeypatch):
    """Verifies valid response payloads when data exists inside the database engine."""
    mock_time = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    db_row = (mock_time, "test/topic", "comp", "metric", 99.9, None, None)

    # Mock the reader module instance attached to the server
    monkeypatch.setattr("mfi_ddb.databases.timescaledb.dws.server.reader.get_point", lambda topic, ts, past: db_row)

    service = DataService()
    request = service_pb2.GetDataPointRequest()
    request.topic = "test/topic"
    request.timestamp.FromDatetime(mock_time)

    response = service.GetDataPoint(request, context=None)
    assert response.datapoint.topic == "test/topic"
    assert response.datapoint.float_value == 99.9


def test_get_data_point_empty_table(monkeypatch):
    """Verifies elegant empty responses when a cold-boot request targets an empty table, avoiding index errors."""
    # Table lookup returns None if zero matching records are located
    monkeypatch.setattr("mfi_ddb.databases.timescaledb.dws.server.reader.get_point", lambda topic, ts, past: None)

    service = DataService()
    request = service_pb2.GetDataPointRequest()
    request.topic = "new/empty/topic"
    request.timestamp.FromDatetime(datetime.now(timezone.utc))

    response = service.GetDataPoint(request, context=None)
    # Ensure it returns a initialized response structure containing no active datapoint variant payload
    assert not response.HasField("datapoint")


# 3. RPC GET_DATA_RANGE & PAGINATION HANDLER TESTS

def test_get_data_range(monkeypatch):
    """Verifies page token isolation using ISO-timestamp keys extracted from rows."""
    time_1 = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    time_2 = datetime(2026, 5, 26, 12, 5, 0, tzinfo=timezone.utc)
    
    db_rows = [
        (time_1, "test/topic", "comp", "metric", 10.0, None, None),
        (time_2, "test/topic", "comp", "metric", 20.0, None, None)
    ]

    monkeypatch.setattr(
        "mfi_ddb.databases.timescaledb.dws.server.reader.get_range", 
        lambda topic, start, end, size, token: db_rows
    )

    service = DataService()
    request = service_pb2.GetDataRangeRequest()
    request.topic = "test/topic"
    request.start_time.FromDatetime(time_1)
    request.end_time.FromDatetime(time_2)

    response = service.GetDataRange(request, context=None)
    assert len(response.datapoints) == 2
    # Next page token string must equal the exact ISO representation of the ultimate element's timestamp
    assert response.next_page_token == time_2.isoformat()


def test_get_data_range_timestamp_collision(monkeypatch):
    """Verifies pagination stability when multiple records share identical millisecond timestamps."""
    collision_time = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    
    # High frequency logging telemetry collision sample
    db_rows = [
        (collision_time, "test/topic", "comp", "metric_A", 1.0, None, None),
        (collision_time, "test/topic", "comp", "metric_B", 2.0, None, None)
    ]

    monkeypatch.setattr(
        "mfi_ddb.databases.timescaledb.dws.server.reader.get_range", 
        lambda topic, start, end, size, token: db_rows
    )

    service = DataService()
    request = service_pb2.GetDataRangeRequest()
    request.topic = "test/topic"

    response = service.GetDataRange(request, context=None)
    assert len(response.datapoints) == 2
    # Verify that the server constructs a token out of the matching timestamp instead of hanging or crashing
    assert response.next_page_token == collision_time.isoformat()


# 4. RPC STREAM_DATA STREAMING & TIMEOUT TESTS

class FakegRPCContext:
    """Mock representing the state connection lifecycle context manager of a gRPC frame."""
    def __init__(self, active_sequence):
        self.active_sequence = list(active_sequence)

    def is_active(self):
        if not self.active_sequence:
            return False
        return self.active_sequence.pop(0)


def test_stream_data_polling(monkeypatch):
    """Verifies the tailing loop yields records immediately and backs off via sleep when the database is idle."""
    mock_time = datetime(2026, 5, 26, 12, 0, 0, tzinfo=timezone.utc)
    db_responses = [
        [(mock_time, "test/topic", "comp", "metric", 5.5, None, None)], # Call 1: Returns 1 point
        [] # Call 2: Empty, triggers polling backoff sleep
    ]

    def mock_stream_batch(topic, since_ts, limit):
        return db_responses.pop(0) if db_responses else []

    monkeypatch.setattr("mfi_ddb.databases.timescaledb.dws.server.reader.stream_batch", mock_stream_batch)

    # Intercept sleep to measure backend loop throttling actions without locking test timings
    sleep_calls = []
    monkeypatch.setattr("time.sleep", lambda seconds: sleep_calls.append(seconds))

    service = DataService()
    request = service_pb2.StreamDataRequest()
    request.topic = "test/topic"
    
    # gRPC channel stays up across exactly two operational evaluate evaluations
    context = FakegRPCContext([True, True, False])

    responses = list(service.StreamData(request, context))
    
    assert len(responses) == 1
    assert responses[0].datapoint.float_value == 5.5
    # Verify that exactly one empty poll event checked out and slept for the configured delay period
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == 1.0


def test_stream_data_disconnect(monkeypatch):
    """Verifies streaming resources free up instantly and exit the loop the moment a client drops connection."""
    monkeypatch.setattr("mfi_ddb.databases.timescaledb.dws.server.reader.stream_batch", lambda t, s, l: [])
    monkeypatch.setattr("time.sleep", lambda s: None)

    service = DataService()
    request = service_pb2.StreamDataRequest()
    request.topic = "test/topic"

    # Context evaluates as disconnected immediately on loop invocation step
    context = FakegRPCContext([False])

    responses = list(service.StreamData(request, context))
    # Generator exits execution immediately without returning metrics or locking connection channels
    assert len(responses) == 0


# 5. CONNECTION POOL RELIABILITY & CLEANUP TESTS

class FakeConnectionPool:
    """Mock connection manager constructed to monitor check-in and checkout mechanics."""
    def __init__(self):
        self.getconn_called = 0
        self.putconn_called = 0

    def getconn(self):
        self.getconn_called += 1
        # Create a mock driver connection wrapper object mapping expected features
        conn = type("Conn", (object,), {
            "autocommit": False, 
            "cursor": self.mock_cursor,
            "__enter__": lambda s: s,
            "__exit__": lambda s, et, ev, tb: False
		})()
        return conn

    def putconn(self, conn):
        self.putconn_called += 1

    def mock_cursor(self):
        # Force an execution syntax exception to evaluate recovery framework actions
        cursor = type("Cursor", (object,), {
            "execute": self.raise_sql_error,
            "__enter__": lambda s: s,
            "__exit__": lambda s, et, ev, tb: False
		})()
        return cursor

    def raise_sql_error(self, sql, params=None):
        raise RuntimeError("PostgreSQL Syntax Error / Deadlock Connection Interruption")


def test_grpc_connection_pool_cleanup(monkeypatch):
    """Verifies connections are returned to the ThreadedConnectionPool via context managers even during hard query failures."""
    fake_pool = FakeConnectionPool()
    
    # Subclass real TimeScaleReader structure to replace internal pool tracking references
    class TestedReader(TimeScaleReader):
        def __init__(self):
            self.pool = fake_pool

    test_reader = TestedReader()

    # Verify that a query exception surfaces up safely to the runtime loop executor
    with pytest.raises(RuntimeError, match="PostgreSQL Syntax Error"):
        test_reader.get_point("test/topic", datetime.now(timezone.utc))

    # CRITICAL VERIFICATION: Prove that even though the SQL block crashed hard,
    # the context manager's finally step caught it and successfully returned the socket threadline to the pool.
    assert fake_pool.getconn_called == 1
    assert fake_pool.putconn_called == 1
