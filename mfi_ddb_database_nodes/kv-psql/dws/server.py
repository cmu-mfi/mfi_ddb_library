#!/usr/bin/env python3
"""
Database Web Service (DWS) Server for PostgreSQL Key-Value Store
Exposes gRPC services for data retrieval.
"""

import argparse
import logging
import os
import sys
import threading
import time
from concurrent import futures
from datetime import datetime, timezone
from typing import Optional, Tuple

import grpc
import yaml

# Add the gen directory to the path for imports
gen_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gen')
if gen_dir not in sys.path:
    sys.path.insert(0, gen_dir)

# Import protobuf modules
import psycopg2
from gen import models_pb2, models_pb2_grpc, service_pb2, service_pb2_grpc

# Import timestamp for proto conversion
from google.protobuf import timestamp_pb2
from psycopg2 import sql

logger = logging.getLogger(__name__)


from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter  # <-- 1. Added get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
meter = get_meter("mfi.kv_psql.dws")

grpc_requests_counter = meter.create_counter(
    "mfi_kv_dws_grpc_requests_total",
    description="Total gRPC requests processed by KV-PSQL DWS"
)

grpc_request_duration = meter.create_histogram(
    "mfi_kv_dws_grpc_duration_seconds",
    description="Duration of gRPC request processing in seconds"
)

db_connection_failures = meter.create_counter(
    "mfi_kv_dws_db_connection_failures_total",
    description="Total failed database connection attempts"
)

returned_rows_histogram = meter.create_histogram(
    "mfi_kv_dws_returned_rows",
    description="Number of database rows returned per retrieval request"
)
# ==========================================


class DataServiceServicer(service_pb2_grpc.DataServiceServicer):
    """Implementation of the DataService gRPC service."""
    
    def __init__(self, db_config: dict, debug: bool = False):
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
        self._setup_logging(debug)
        
        conn = self._get_connection()
        if not conn:
            raise Exception("Unable to connect to the kv database") 
        else:
            self.logger.info("kv database connection success!")
        
        # Cache for database connections
        self._db_cache: dict = {}
        
    def _setup_logging(self, debug: bool = False):
        """Configure logging format and level."""
        level = logging.DEBUG if debug else logging.INFO
        logging.basicConfig(
            level=level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    def _get_connection(self) -> Optional[psycopg2.extensions.connection]:
        """Get a database connection."""
        try:
            return psycopg2.connect(
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 5432),
                database=self.db_config.get('database', 'mfi_kv'),
                user=self.db_config.get('user', 'mfi'),
                password=self.db_config.get('password', 'mfiddb')
            )
        except psycopg2.Error as e:
            db_connection_failures.add(1)  # <-- 2. Record connection failures
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            return None
    
    def _timestamp_to_datetime(self, timestamp_msg) -> datetime:
        """Convert protobuf Timestamp to Python datetime."""
        return datetime.fromtimestamp(
            timestamp_msg.seconds + timestamp_msg.nanos / 1e9,
            tz=timezone.utc
        )
    
    def _datapoint_to_proto(self, row: tuple) -> models_pb2.Datapoint:
        """Convert database row to protobuf Datapoint."""
        timestamp, topic, payload = row
        
        # Create Timestamp message using google.protobuf.Timestamp
        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(timestamp)
        
        # Create Datapoint message
        datapoint = models_pb2.Datapoint(
            topic=topic,
            timestamp=ts
        )
        
        # Determine payload type and set appropriately
        if isinstance(payload, dict):
            datapoint.json_value.update(payload)
        elif isinstance(payload, int):
            datapoint.int_value = payload
        elif isinstance(payload, float):
            datapoint.float_value = payload
        elif isinstance(payload, str):
            datapoint.string_value = payload
        else:
            datapoint.string_value = str(payload)
        
        return datapoint
    
    def GetDataPoint(self, request, context):
        """Retrieve a single datapoint for a specific topic at an exact timestamp."""
        start_time = time.perf_counter()  # <-- 3. Start latency timer
        
        conn = self._get_connection()
        if not conn:
            # Record failed request (No Database Connection)
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "ERROR"})
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to connect to database")
            return service_pb2.GetDataPointResponse()
        
        try:
            cursor = conn.cursor()
            
            # Convert timestamp
            target_time = self._timestamp_to_datetime(request.timestamp)
            
            # Determine if we are doing exact matching or regex matching
            topic_clause, topic_param, is_wildcard = self._topic_clause(request.topic)
            
            # Get exact match or closest past/future based on request direction
            if not request.do_closest_past:
                order = "ASC"
                op = ">="
            else:
                order = "DESC"
                op = "<="

            limit_clause = "" if is_wildcard else "LIMIT 1"

            query = f"""
                SELECT timestamp, topic, payload
                FROM kv_data
                WHERE {topic_clause} AND timestamp {op} %s
                ORDER BY timestamp {order}
                {limit_clause}
            """
            
            # Execute query
            cursor.execute(query, (topic_param, target_time))
            rows = cursor.fetchall()
            
            # For wildcard topics, verify exactly one topic matches
            if is_wildcard and len(rows) != 1:
                unique_topics = set(row[1] for row in rows)
                if len(unique_topics) == 0:
                    duration = time.perf_counter() - start_time
                    grpc_request_duration.record(duration, {"method": "GetDataPoint"})
                    grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "NOT_FOUND"})
                    
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("No datapoint found")
                    return service_pb2.GetDataPointResponse()
                elif len(unique_topics) > 1:
                    duration = time.perf_counter() - start_time
                    grpc_request_duration.record(duration, {"method": "GetDataPoint"})
                    grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "INVALID_ARGUMENT"})
                    
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Topic pattern matches {len(unique_topics)} distinct topics, expected exactly 1")
                    return service_pb2.GetDataPointResponse()
            
            duration = time.perf_counter() - start_time  # <-- 4. Stop latency timer
            
            if rows:
                grpc_request_duration.record(duration, {"method": "GetDataPoint"})
                grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "OK"})
                returned_rows_histogram.record(1, {"method": "GetDataPoint"})
                
                datapoint = self._datapoint_to_proto(rows[0])
                return service_pb2.GetDataPointResponse(datapoint=datapoint)
            else:
                grpc_request_duration.record(duration, {"method": "GetDataPoint"})
                grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "NOT_FOUND"})
                
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("No datapoint found")
                return service_pb2.GetDataPointResponse()
                
        except psycopg2.Error as e:
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "ERROR"})
            
            self.logger.error(f"Database error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return service_pb2.GetDataPointResponse()
        finally:
            conn.close()
    
    def _topic_to_regex(self, topic_pattern: str) -> str:
        """Translate an MQTT topic filter into a full-match PostgreSQL regex.
        
        MQTT wildcards are intentionally mapped with their topic-level semantics:
        '+' matches exactly one topic level and '#' matches zero or more levels.
        """
        regex_parts = []
        regex_meta = set('.^$*?{}[]\\|()')
        
        for ch in topic_pattern:
            if ch == "+":
                regex_parts.append(r"[^/]+")
            elif ch == "#":
                regex_parts.append(r".*")
            else:
                if ch in regex_meta:
                    regex_parts.append("\\" + ch)
                else:
                    regex_parts.append(ch)
                    
        return "^" + "".join(regex_parts) + "$"

    def _topic_clause(self, topic_pattern: str) -> Tuple[str, str, bool]:
        """Return the SQL topic clause, the bound parameter, and a wildcard flag.
        
        Exact topics keep the fast equality predicate (=). MQTT wildcard filters 
        are expanded into a regex match (~).
        """
        if "+" not in topic_pattern and "#" not in topic_pattern:
            return "topic = %s", topic_pattern, False
        return "topic ~ %s", self._topic_to_regex(topic_pattern), True

    def GetDataRange(self, request, context):
        """Retrieve a list of datapoints between start_time and end_time."""
        start_time_counter = time.perf_counter()  # <-- 5. Start latency timer
        
        conn = self._get_connection()
        if not conn:
            duration = time.perf_counter() - start_time_counter
            grpc_request_duration.record(duration, {"method": "GetDataRange"})
            grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "ERROR"})
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to connect to database")
            return service_pb2.GetDataRangeResponse()
        
        try:
            cursor = conn.cursor()
            
            # Convert timestamps
            start_time = self._timestamp_to_datetime(request.start_time)
            end_time = self._timestamp_to_datetime(request.end_time)
            page_size = request.page_size if request.page_size > 0 else 1000
            page_token = request.page_token
            
            # Generate the dynamic topic clause and parameter
            topic_clause, topic_param, _ = self._topic_clause(request.topic)
            
            # Build query with pagination
            if page_token:
                try:
                    start_from = datetime.fromtimestamp(float(page_token), tz=timezone.utc)
                    time_filter = "timestamp > %s AND timestamp <= %s"
                    params = (start_from, end_time)
                except (ValueError, OSError):
                    duration = time.perf_counter() - start_time_counter
                    grpc_request_duration.record(duration, {"method": "GetDataRange"})
                    grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "INVALID_ARGUMENT"})
                    
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Invalid page token")
                    return service_pb2.GetDataRangeResponse()
            else:
                time_filter = "timestamp >= %s AND timestamp <= %s"
                params = (start_time, end_time)
            
            count_query = f"""
                SELECT COUNT(*) FROM kv_data WHERE {topic_clause} AND {time_filter}
            """
            
            data_query = f"""
                SELECT timestamp, topic, payload
                FROM kv_data
                WHERE {topic_clause} AND {time_filter}
                ORDER BY timestamp ASC
                LIMIT %s
            """
            
            cursor.execute(count_query, (topic_param, *params))
            count_result = cursor.fetchone()
            total_count = count_result[0] if count_result else 0
            
            cursor.execute(data_query, (topic_param, *params, page_size))
            rows = cursor.fetchall()
            
            # Convert to protobuf messages
            datapoints = [self._datapoint_to_proto(row) for row in rows]
            
            # Generate next page token if there are more results
            next_page_token = ""
            if len(rows) == page_size and total_count > len(rows):
                last_timestamp = rows[-1][0]
                next_page_token = str(last_timestamp.timestamp())
            
            duration = time.perf_counter() - start_time_counter  # <-- 6. Stop latency timer
            grpc_request_duration.record(duration, {"method": "GetDataRange"})
            grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "OK"})
            returned_rows_histogram.record(len(datapoints), {"method": "GetDataRange"})
            
            return service_pb2.GetDataRangeResponse(
                datapoints=datapoints,
                next_page_token=next_page_token
            )
            
        except psycopg2.Error as e:
            duration = time.perf_counter() - start_time_counter
            grpc_request_duration.record(duration, {"method": "GetDataRange"})
            grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "ERROR"})
            
            self.logger.error(f"Database error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return service_pb2.GetDataRangeResponse()
        finally:
            conn.close()
    
    def StreamData(self, request, context):
        """Stream datapoints in real-time."""
        conn = self._get_connection()
        if not conn:
            grpc_requests_counter.add(1, {"method": "StreamData", "status": "ERROR"})
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to connect to database")
            return
        
        try:
            cursor = conn.cursor()
            
            # Check if start_from timestamp is provided
            start_from = None
            if request.HasField('start_from'):
                start_from = self._timestamp_to_datetime(request.start_from)
            
            # Get the current timestamp to start streaming from
            if start_from is None:
                cursor.execute("SELECT NOW() AT TIME ZONE 'UTC'")
                time_result = cursor.fetchone()
                start_from = time_result[0] if time_result else datetime.now(timezone.utc)
            
            topic_clause, topic_param, _ = self._topic_clause(request.topic)
            self.logger.info(f"Starting stream for topic filter: {request.topic} from {start_from}")
            
            grpc_requests_counter.add(1, {"method": "StreamData", "status": "OK"})
            
            while context.is_active():
                query = f"""
                    SELECT timestamp, topic, payload
                    FROM kv_data
                    WHERE {topic_clause} AND timestamp > %s
                    ORDER BY timestamp ASC
                    LIMIT 100
                """
                
                cursor.execute(query, (topic_param, start_from))
                rows = cursor.fetchall()
                
                if not rows:
                    time.sleep(0.1)
                    continue

                # Record the stream packet chunk sizes
                returned_rows_histogram.record(len(rows), {"method": "StreamData"})

                for row in rows:
                    datapoint = self._datapoint_to_proto(row)
                    yield service_pb2.StreamDataResponse(datapoint=datapoint)
                    start_from = row[0]
                    
        except psycopg2.Error as e:
            grpc_requests_counter.add(1, {"method": "StreamData", "status": "ERROR"})
            self.logger.error(f"Database error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
        finally:
            conn.close()


def serve(db_config: dict, port: int = 50051, debug: bool = False):
    """Start the gRPC server."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_DataServiceServicer_to_server(
        DataServiceServicer(db_config, debug=debug), server
    )
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    
    logging.info(f"DWS Server started on port {port}")
        
    try:
        while True:
            time.sleep(86400)
    except KeyboardInterrupt:
        server.stop(0)
        logging.info("DWS Server stopped")


def main():    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='DWS Server for PostgreSQL')
    parser.add_argument('-v', '--verbose', action='store_true', 
                        help='Enable debug logging')
    args = parser.parse_args()
    
    # Load configuration
    try:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        with open(os.path.join(script_dir,'config.yaml'), 'r') as f:
            config = yaml.safe_load(f)
    except FileNotFoundError:
        logger.error("ERROR: config.yaml not found")
        config = {'postgres': {}}
    
    db_config = config.get('postgres', {})
    
    port = config.get('dws', {}).get('port', 50051)
    
    serve(db_config, port, debug=args.verbose)


if __name__ == '__main__':
    main()