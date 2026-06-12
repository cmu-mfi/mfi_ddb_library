#!/usr/bin/env python3
"""
Pytest-based tests for the DWS server with PostgreSQL.

Tests use a temporary test database that is created before tests run
and cleaned up after all tests complete.
"""
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import pytest

# Add kv-psql root directory to path for imports
# This allows imports like 'from dws.gen import ...'
script_dir = os.path.dirname(os.path.abspath(__file__))
kvpsql_dir = os.path.abspath(os.path.join(script_dir, '..'))
if kvpsql_dir not in sys.path:
    sys.path.insert(0, kvpsql_dir)

import grpc
from google.protobuf import timestamp_pb2
from dws.gen import models_pb2, models_pb2_grpc, service_pb2, service_pb2_grpc


def get_db_config():
    """Get database config from environment or use defaults."""
    return {
        'host': os.environ.get('TEST_DB_HOST', 'localhost'),
        'port': int(os.environ.get('TEST_DB_PORT', 5432)),
        'database': os.environ.get('TEST_DB_NAME', 'test_mfi_kv'),
        'user': os.environ.get('TEST_DB_USER', 'mfi'),
        'password': os.environ.get('TEST_DB_PASSWORD', 'mfiddb'),
    }


TEST_DB_CONFIG = get_db_config()


def get_test_connection():
    """Create a connection to the test database."""
    import psycopg2
    return psycopg2.connect(**TEST_DB_CONFIG)


def setup_test_db():
    """Create the test database and schema."""
    import psycopg2
    
    # Connect to postgres database to create test database
    conn = psycopg2.connect(
        host=TEST_DB_CONFIG.get('host'),
        port=TEST_DB_CONFIG.get('port', 5432),
        database='postgres',
        user=TEST_DB_CONFIG.get('user'),
        password=TEST_DB_CONFIG.get('password')
    )
    conn.autocommit = True
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DB_CONFIG['database']}")
            cursor.execute(f"CREATE DATABASE {TEST_DB_CONFIG['database']}")
    finally:
        conn.close()
    
    # Connect to test database and create schema
    conn = get_test_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS kv_data (
                    id SERIAL PRIMARY KEY,
                    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    topic TEXT NOT NULL,
                    payload JSONB NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kv_data_timestamp 
                ON kv_data (timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kv_data_topic 
                ON kv_data (topic)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kv_data_topic_timestamp 
                ON kv_data (topic, timestamp)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_kv_data_created_at 
                ON kv_data (created_at)
            """)
        conn.commit()
    finally:
        conn.close()


def teardown_test_db():
    """Drop the test database."""
    import psycopg2
    
    conn = psycopg2.connect(
        host=TEST_DB_CONFIG.get('host'),
        port=TEST_DB_CONFIG.get('port', 5432),
        database='postgres',
        user=TEST_DB_CONFIG.get('user'),
        password=TEST_DB_CONFIG.get('password')
    )
    conn.autocommit = True
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(f"DROP DATABASE IF EXISTS {TEST_DB_CONFIG['database']}")
    finally:
        conn.close()


def insert_test_datapoints():
    """Insert test data points into the database."""
    conn = get_test_connection()
    try:
        with conn.cursor() as cursor:
            base_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
            
            test_data = [
                (base_time, "mfi/test/topic1", {"value": 100, "unit": "m/s"}),
                (base_time, "mfi/test/topic2", {"temperature": 25.5, "unit": "celsius"}),
                (base_time, "mfi/test/topic3", {"status": "online", "code": 200}),
            ]
            
            for timestamp, topic, payload in test_data:
                cursor.execute(
                    """
                    INSERT INTO kv_data (timestamp, topic, payload)
                    VALUES (%s, %s, %s::jsonb)
                    """,
                    (timestamp, topic, json.dumps(payload))
                )
            
            later_time = base_time.replace(second=30)
            cursor.execute(
                """
                INSERT INTO kv_data (timestamp, topic, payload)
                VALUES (%s, %s, %s::jsonb)
                """,
                (later_time, "mfi/test/topic1", json.dumps({"value": 150, "unit": "m/s"}))
            )
            
            conn.commit()
    finally:
        conn.close()


def timestamp_to_protobuf(dt: datetime) -> timestamp_pb2.Timestamp:
    """Convert datetime to protobuf Timestamp."""
    ts = timestamp_pb2.Timestamp()
    ts.FromDatetime(dt)
    return ts


@pytest.fixture(scope="session", autouse=True)
def test_db_session():
    """Setup and teardown test database for all tests."""
    # Setup
    setup_test_db()
    insert_test_datapoints()
    
    # Yield control to tests
    yield
    
    # Teardown
    teardown_test_db()


@pytest.fixture
def stub_get_data_point():
    """Create gRPC stub for GetDataPoint tests."""
    import threading
    
    # Add dws directory to path for server import
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dws_dir = os.path.abspath(os.path.join(script_dir, '..', 'dws'))
    if dws_dir not in sys.path:
        sys.path.insert(0, dws_dir)
    
    import server
    
    server_thread = threading.Thread(
        target=server.serve,
        args=(TEST_DB_CONFIG, 50052),
        daemon=True
    )
    server_thread.start()
    
    time.sleep(1)
    
    channel = grpc.insecure_channel('localhost:50052')
    stub = service_pb2_grpc.DataServiceStub(channel)
    
    yield stub
    
    channel.close()


@pytest.fixture
def stub_get_data_range():
    """Create gRPC stub for GetDataRange tests."""
    import threading
    
    # Add dws directory to path for server import
    script_dir = os.path.dirname(os.path.abspath(__file__))
    dws_dir = os.path.abspath(os.path.join(script_dir, '..', 'dws'))
    if dws_dir not in sys.path:
        sys.path.insert(0, dws_dir)
    
    import server
    
    server_thread = threading.Thread(
        target=server.serve,
        args=(TEST_DB_CONFIG, 50053),
        daemon=True
    )
    server_thread.start()
    
    time.sleep(1)
    
    channel = grpc.insecure_channel('localhost:50053')
    stub = service_pb2_grpc.DataServiceStub(channel)
    
    yield stub
    
    channel.close()


class TestGetDataPoint:
    """Tests for GetDataPoint gRPC method."""
    
    def test_get_data_point_exact_match(self, stub_get_data_point):
        """Test retrieving a datapoint with an exact timestamp match."""
        target_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataPointRequest(
            topic="mfi/test/topic1",
            timestamp=timestamp_to_protobuf(target_time),
            do_closest_past=True
        )
        
        response = stub_get_data_point.GetDataPoint(request)
        
        assert response.datapoint.topic == "mfi/test/topic1"
        assert response.datapoint.timestamp.seconds > 0
        assert response.datapoint.json_value.fields["value"].number_value == 100
        assert response.datapoint.json_value.fields["unit"].string_value == "m/s"
    
    def test_get_data_point_closest_past(self, stub_get_data_point):
        """Test retrieving closest datapoint in the past."""
        target_time = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataPointRequest(
            topic="mfi/test/topic1",
            timestamp=timestamp_to_protobuf(target_time),
            do_closest_past=True
        )
        
        response = stub_get_data_point.GetDataPoint(request)
        
        assert response.datapoint.json_value.fields["value"].number_value == 100
    
    def test_get_data_point_closest_future(self, stub_get_data_point):
        """Test retrieving closest datapoint in the future."""
        target_time = datetime(2024, 1, 1, 12, 0, 15, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataPointRequest(
            topic="mfi/test/topic1",
            timestamp=timestamp_to_protobuf(target_time),
            do_closest_past=False
        )
        
        response = stub_get_data_point.GetDataPoint(request)
        
        assert response.datapoint.json_value.fields["value"].number_value == 150
    
    def test_get_data_point_not_found(self, stub_get_data_point):
        """Test retrieving a non-existent datapoint."""
        target_time = datetime(2020, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataPointRequest(
            topic="mfi/test/topic1",
            timestamp=timestamp_to_protobuf(target_time),
            do_closest_past=True
        )
        
        with pytest.raises(grpc.RpcError) as exc_info:
            stub_get_data_point.GetDataPoint(request)
        
        assert exc_info.value.code() == grpc.StatusCode.NOT_FOUND
    
    def test_get_data_point_wildcard(self, stub_get_data_point):
        """Test retrieving a datapoint using MQTT wildcard topic matching."""
        target_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        
        # Insert test data with a topic that matches a wildcard pattern
        conn = get_test_connection()
        try:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO kv_data (timestamp, topic, payload)
                    VALUES (%s, %s, %s::jsonb)
                    """,
                    (target_time, "mfi-v1.0-kv/CMU/machine-a/sensor1", json.dumps({"value": 42, "unit": "psi"}))
                )
                conn.commit()
        finally:
            conn.close()
        
        # Use wildcard topic to match the inserted data
        request = service_pb2.GetDataPointRequest(
            topic="mfi-v1.0-kv/CMU/machine-a/#",
            timestamp=timestamp_to_protobuf(target_time),
            do_closest_past=True
        )
        
        response = stub_get_data_point.GetDataPoint(request)
        
        assert response.datapoint.topic == "mfi-v1.0-kv/CMU/machine-a/sensor1"
        assert response.datapoint.timestamp.seconds > 0
        assert response.datapoint.json_value.fields["value"].number_value == 42
        assert response.datapoint.json_value.fields["unit"].string_value == "psi"


class TestGetDataRange:
    """Tests for GetDataRange gRPC method."""
    
    def test_get_data_range_basic(self, stub_get_data_range):
        """Test basic range query."""
        start_time = datetime(2024, 1, 1, 11, 59, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataRangeRequest(
            topic="mfi/test/topic1",
            start_time=timestamp_to_protobuf(start_time),
            end_time=timestamp_to_protobuf(end_time),
            page_size=100
        )
        
        response = stub_get_data_range.GetDataRange(request)
        
        assert len(response.datapoints) == 2
    
    def test_get_data_range_empty(self, stub_get_data_range):
        """Test range query with no results."""
        start_time = datetime(2020, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2020, 1, 1, 23, 59, 59, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataRangeRequest(
            topic="mfi/test/topic1",
            start_time=timestamp_to_protobuf(start_time),
            end_time=timestamp_to_protobuf(end_time),
            page_size=100
        )
        
        response = stub_get_data_range.GetDataRange(request)
        
        assert len(response.datapoints) == 0
    
    def test_get_data_range_pagination(self, stub_get_data_range):
        """Test pagination with page_size."""
        # Insert more test data for pagination test
        conn = get_test_connection()
        try:
            with conn.cursor() as cursor:
                base_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
                for i in range(10):
                    cursor.execute(
                        """
                        INSERT INTO kv_data (timestamp, topic, payload)
                        VALUES (%s, %s, %s::jsonb)
                        """,
                        (base_time.replace(minute=i), "mfi/test/pagination", json.dumps({"index": i}))
                    )
                conn.commit()
        finally:
            conn.close()
        
        # Request with small page size
        start_time = datetime(2024, 1, 1, 13, 0, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 13, 10, 0, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataRangeRequest(
            topic="mfi/test/pagination",
            start_time=timestamp_to_protobuf(start_time),
            end_time=timestamp_to_protobuf(end_time),
            page_size=3
        )
        
        response = stub_get_data_range.GetDataRange(request)
        
        assert len(response.datapoints) == 3
        assert response.next_page_token != ""
    
    def test_get_data_range_multiple_topics(self, stub_get_data_range):
        """Test range query returns data for different topics."""
        start_time = datetime(2024, 1, 1, 11, 59, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        
        request = service_pb2.GetDataRangeRequest(
            topic="mfi/test/topic2",
            start_time=timestamp_to_protobuf(start_time),
            end_time=timestamp_to_protobuf(end_time),
            page_size=100
        )
        
        response = stub_get_data_range.GetDataRange(request)
        
        assert len(response.datapoints) == 1
        assert response.datapoints[0].json_value.fields["temperature"].number_value == 25.5

    def test_get_data_range_wildcard(self, stub_get_data_range):
        """Test range query with MQTT wildcard topic matching."""
        start_time = datetime(2024, 1, 1, 11, 59, 0, tzinfo=timezone.utc)
        end_time = datetime(2024, 1, 1, 12, 1, 0, tzinfo=timezone.utc)
        
        # Use wildcard topic to match all topics under "mfi/test/"
        request = service_pb2.GetDataRangeRequest(
            topic="mfi/test/#",
            start_time=timestamp_to_protobuf(start_time),
            end_time=timestamp_to_protobuf(end_time),
            page_size=100
        )
        
        response = stub_get_data_range.GetDataRange(request)
        
        # Should return 4 datapoints: topic1 (2 entries) + topic2 (1 entry) + topic3 (1 entry)
        assert len(response.datapoints) == 4


if __name__ == "__main__":
    pytest.main([__file__, "-v"])