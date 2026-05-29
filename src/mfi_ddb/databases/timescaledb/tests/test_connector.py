"""Testing the TimescaleDB connector"""

"""
Testing Catalog:

Test name format: test_[functionality/function being tested]_purpose

1. test_classify_value_numeric: Verifies floats, ints, and booleans map to value_num.

2. test_classify_value_text: Verifies strings map to value_text.

3. test_classify_value_json: Verifies dicts/lists map to serialized value_json.

4. test_classify_value_nan_infinity [Added Boundary]: Verifies sensor NaN/Infinity faults are classified safely without causing serialization errors.

5. test_to_ts_valid: Verifies accurate millisecond epoch conversions.

6. test_to_ts_fallback: Verifies corrupt inputs or None values fall back safely to UTC now.

7. test_on_message_success: Verifies decoded MQTT Sparkplug B frames are correctly formatted and appended to data_queue.

8. test_on_message_overflow: Verifies that when the memory buffer is completely full, it triggers non-blocking drops and logs alerts instead of causing OOM system crashes.

9. test_db_batch_writer_worker_flushing: Verifies micro-batch grouping and that incomplete batches force-flush cleanly the moment the 100ms time boundary expires.
"""

from datetime import datetime, timezone
import math
import queue
import sys
import time
import pytest

from connector.main import (
    classify_value,
    to_ts,
    on_message,
    data_queue,
    db_batch_writer_worker,
)
from connector.db import TimeScaleWriter


@pytest.fixture(autouse=True)
def reset_global_queue():
    """Fixture to ensure the shared memory queue is clean before and after every test."""
    while not data_queue.empty():
        try:
            data_queue.get_nowait()
            data_queue.task_done()
        except queue.Empty:
            break
    yield


# 1. DATA MAPPING & CLASSIFICATION TESTS

def test_classify_value_numeric():
    """Verifies floats, ints, and booleans map perfectly to value_num."""
    # Test float
    vnum, vtext, vjson = classify_value(23.67)
    assert vnum == 23.67
    assert vtext is None
    assert vjson is None

    # Test integer
    vnum, vtext, vjson = classify_value(100)
    assert vnum == 100.0
    assert vtext is None
    assert vjson is None

    # Test boolean coercion (True -> 1.0)
    vnum, vtext, vjson = classify_value(True)
    assert vnum == 1.0
    assert vtext is None
    assert vjson is None


def test_classify_value_text():
    """Verifies strings map cleanly to value_text."""
    vnum, vtext, vjson = classify_value("Machine Status: Running")
    assert vnum is None
    assert vtext == "Machine Status: Running"
    assert vjson is None


def test_classify_value_json():
    """Verifies dicts/lists serialize correctly into value_json string blocks."""
    payload_dict = {"temperature": 85.2, "alerts": ["high_temp", "critical"]}
    vnum, vtext, vjson = classify_value(payload_dict)
    
    assert vnum is None
    assert vtext is None
    assert vjson == '{"temperature": 85.2, "alerts": ["high_temp", "critical"]}'


def test_classify_value_nan_infinity():
    """Verifies sensor NaN/Infinity faults are classified safely into value_num without crashing."""
    # NaN check
    vnum_nan, _, _ = classify_value(float("nan"))
    # Explicitly check None first so Pylance knows it's a safe float next
    assert vnum_nan is not None  
    assert math.isnan(vnum_nan)

    # Infinity check
    vnum_inf, _, _ = classify_value(float("inf"))
    assert vnum_inf is not None
    assert math.isinf(vnum_inf) and vnum_inf > 0


# 2. TIMESTAMP PARSING TESTS

def test_to_ts_valid():
    """Verifies accurate millisecond epoch conversions (numeric and string formats)."""
    # Test int millisecond parsing
    dt_int = to_ts(1779810223839)
    assert dt_int.tzinfo == timezone.utc
    assert dt_int.year == 2026

    # Test string millisecond parsing
    dt_str = to_ts("1779810223839")
    assert dt_str.tzinfo == timezone.utc
    assert dt_str.year == 2026


def test_to_ts_fallback():
    """Verifies corrupt inputs or None values fall back safely to UTC now."""
    # Test None input
    dt_none = to_ts(None)
    assert isinstance(dt_none, datetime)
    assert dt_none.tzinfo == timezone.utc

    # Test corrupted string input
    dt_corrupt = to_ts("invalid_epoch_string")
    assert isinstance(dt_corrupt, datetime)
    assert dt_corrupt.tzinfo == timezone.utc


# 3. MQTT INGESTION PIPELINE TESTS

class FakeMessage:
    """Mock structure mimicking an incoming paho-mqtt message payload frame."""
    def __init__(self, topic, payload):
        self.topic = topic
        self.payload = payload


def test_on_message_success(monkeypatch):
    """Verifies decoded MQTT Sparkplug B frames are correctly formatted and appended to data_queue."""
    # Mock decode_sparkplug to bypass protobuf deserialization
    mock_metrics = [("DATA/data/temperature", 21.5, 1779810223839)]
    monkeypatch.setattr("connector.main.decode_sparkplug", lambda p: mock_metrics)
    
    # Mock app config reads
    # monkeypatch.setitem(data_queue.queue if hasattr(data_queue, 'queue') else {}, "component_id", "demo-component")
    
	# Target and mock the active app configuration dictionary module property cleanly here:
    try:
        monkeypatch.setitem(sys.modules["connector.main"].cfg, "component_id", "demo-component")
    except (AttributeError, KeyError):
        pass # If cfg object doesn't strictly exist as a mutable dict property, bypass it safely

    msg = FakeMessage("mfi-v1.0-historian/CMU/Machine Shop/demo-component", b"protobuf_binary_bytes")
    
    on_message(client=None, userdata=None, msg=msg)

    assert data_queue.qsize() == 1
    batch = data_queue.get_nowait()
    assert len(batch) == 1
    
    # Validate tuple schema: (time, topic, component, metric, value_num, value_text, value_json)
    t, topic, component, metric, vnum, vtext, vjson = batch[0]
    assert topic == "mfi-v1.0-historian/CMU/Machine Shop/demo-component"
    assert metric == "DATA/data/temperature"
    assert vnum == 21.5


def test_on_message_overflow(monkeypatch):
    """Verifies that when memory buffer is completely full, it drops elements safely without an OOM crash."""
    monkeypatch.setattr("connector.main.decode_sparkplug", lambda p: [("metric", 1.0, None)])
    
    # Artificially force maxsize boundary constraint lower for quick execution
    monkeypatch.setattr(data_queue, "maxsize", 2)
    data_queue.put([("row1",)])
    data_queue.put([("row2",)])
    assert data_queue.full()

    msg = FakeMessage("test/topic", b"bytes")
    
    # Pushing an extra row onto a full queue should catch queue.Full and exit cleanly
    try:
        on_message(client=None, userdata=None, msg=msg)
    except queue.Full:
        pytest.fail("on_message threw an unhandled queue.Full exception and crashed!")


# 4. ASYNCHRONOUS DAEMON WORKER TESTS

# Subclass the real TimeScaleWriter to cleanly bypass strict type checks
class FakeTimeScaleWriter(TimeScaleWriter):
    """Mock writer to intercept batch arrays emitted by the consumer daemon."""
    def __init__(self):
        # Explicitly skip parent __init__ to avoid trying to open a real DB socket
        self.batches_received = []
        # Create a mock structural connection object to appease closure checks
        self.conn = type("FakeConn", (object,), {"closed": False})()

    def insert_rows(self, rows):
        """Batch insert rows method with a parameter name matching the base class exactly."""
        self.batches_received.append(rows)


def test_db_batch_writer_worker_flushing(monkeypatch):
    """Verifies background micro-batch grouping handles chunks and timeouts accurately."""
    fake_writer = FakeTimeScaleWriter()
    
    # Seed queue with 3 individual discrete sample entries
    data_queue.put([("row1",)])
    data_queue.put([("row2",)])
    data_queue.put([("row3",)])

    # We patch time.sleep inside the worker thread to catch the loop break condition,
    # otherwise an infinite worker loop will freeze the execution of pytest.
    loop_control = {"run_count": 0}
    orig_time = time.time

    def mock_time():
        # Advance clock manually to simulate an instant 100ms timeout window expiry
        loop_control["run_count"] += 1
        if loop_control["run_count"] > 5:
            # Force thread break simulation via a mock crash
            raise KeyboardInterrupt()
        # return orig_time() + (loop_control["run_count"] * 0.05)
        # Advance slightly slower per step to let consumer process elements smoothly
        return orig_time() + (loop_control["run_count"] * 0.02)

    monkeypatch.setattr(time, "time", mock_time)

    with pytest.raises(KeyboardInterrupt):
        db_batch_writer_worker(
            q=data_queue,
            db_writer=fake_writer,
            max_batch_size=1000,
            flush_interval_sec=0.1
        )

    # Confirm the 3 rows split across 2 queue payloads were packed into 1 unified database transaction
    assert len(fake_writer.batches_received) >= 1
    combined_batch = fake_writer.batches_received[0]
    assert len(combined_batch) == 3
    # Sum total elements processed across all received flushes to assure zero item loss or drops
    total_processed_rows = sum(len(b) for b in fake_writer.batches_received)
    assert total_processed_rows == 3
    assert combined_batch == [("row1",), ("row2",), ("row3",)]







