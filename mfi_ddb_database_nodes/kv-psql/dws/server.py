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
from opentelemetry.metrics import set_meter_provider
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


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
            # datapoint.json_value = payload
            # cant overwrite the struct object, causes a pylance mismatch error for type
            # calling update will safely read the dictionary and maps it s key value pairs direcly into that preexisting struct object.
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
        conn = self._get_connection()
        if not conn:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details("Failed to connect to database")
            return service_pb2.GetDataPointResponse()
        
        try:
            cursor = conn.cursor()
            
            # Convert timestamp
            target_time = self._timestamp_to_datetime(request.timestamp)
            
            # Check if topic contains wildcard
            # topic_pattern, is_wildcard = self._topic_to_sql_pattern(request.topic)

            # Determine if we are doing exact matching or regex matching
            topic_clause, topic_param, is_wildcard = self._topic_clause(request.topic)
            
            # Get exact match or closest past/future based on request direction
            if not request.do_closest_past:
                order = "ASC"
                op = ">="
            else:
                order = "DESC"
                op = "<="

            # If it's a wildcard match, do not apply 'LIMIT 1' prematurely in case 
            # we need to validate if exactly 1 unique topic matched across results.
            
            limit_clause = "" if is_wildcard else "LIMIT 1"

            # # Get exact match or closest past/future based on request
            # if not request.do_closest_past:
            #     # Get closest datapoint at or after the requested timestamp
            #     if is_wildcard:
            #         query = """
            #             SELECT timestamp, topic, payload
            #             FROM kv_data
            #             WHERE topic LIKE %s AND timestamp >= %s
            #             ORDER BY timestamp ASC
            #         """
            #     else:
            #         query = """
            #             SELECT timestamp, topic, payload
            #             FROM kv_data
            #             WHERE topic = %s AND timestamp >= %s
            #             ORDER BY timestamp ASC
            #             LIMIT 1
            #         """
            # else:
            #     # Get closest datapoint at or before the requested timestamp
            #     if is_wildcard:
            #         query = """
            #             SELECT timestamp, topic, payload
            #             FROM kv_data
            #             WHERE topic LIKE %s AND timestamp <= %s
            #             ORDER BY timestamp DESC
            #         """
            #     else:
            #         query = """
            #             SELECT timestamp, topic, payload
            #             FROM kv_data
            #             WHERE topic = %s AND timestamp <= %s
            #             ORDER BY timestamp DESC
            #             LIMIT 1
            #         """
            
            # Execute query
            # if is_wildcard:
            #     cursor.execute(query, (topic_pattern, target_time))
            # else:
            #     cursor.execute(query, (request.topic, target_time))

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
                # if len(rows) == 0:
                #     context.set_code(grpc.StatusCode.NOT_FOUND)
                #     context.set_details("No datapoint found")
                # else:
                #     context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                #     context.set_details(f"Topic pattern matches {len(rows)} topics, expected exactly 1")
                # return service_pb2.GetDataPointResponse()
                unique_topics = set(row[1] for row in rows)
                if len(unique_topics) == 0:
                    context.set_code(grpc.StatusCode.NOT_FOUND)
                    context.set_details("No datapoint found")
                    return service_pb2.GetDataPointResponse()
                elif len(unique_topics) > 1:
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details(f"Topic pattern matches {len(unique_topics)} distinct topics, expected exactly 1")
                    return service_pb2.GetDataPointResponse()
            
            if rows:
                datapoint = self._datapoint_to_proto(rows[0])
                return service_pb2.GetDataPointResponse(datapoint=datapoint)
            else:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("No datapoint found")
                return service_pb2.GetDataPointResponse()
                
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return service_pb2.GetDataPointResponse()
        finally:
            conn.close()
    
    # def _topic_to_sql_pattern(self, topic: str) -> Tuple[str, bool]:
    #     """Convert MQTT topic to SQL pattern for wildcard matching.
        
    #     Returns (pattern, is_wildcard) tuple.
    #     Converts "mfi/test/#" to "mfi/test%" for LIKE matching.
    #     """
    #     is_wildcard = False
    #     if topic.endswith('/#'):
    #         # Convert MQTT wildcard to SQL LIKE pattern
    #         pattern = topic[:-2] + '%'
    #         is_wildcard = True
    #     elif topic.endswith('#'):
    #         # Handle case like "mfi/test/#" where # is at the end
    #         pattern = topic[:-1] + '%'
    #         is_wildcard = True
    #     else:
    #         pattern = topic
    #     return pattern, is_wildcard
    
    def _topic_to_regex(self, topic_pattern: str) -> str:
        """Translate an MQTT topic filter into a full-match PostgreSQL regex.
        
        MQTT wildcards are intentionally mapped with their topic-level semantics:
        '+' matches exactly one topic level and '#' matches zero or more levels.
        """
        regex_parts = []
        # Escape characters that have special meanings in regex, excluding hyphen
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
        conn = self._get_connection()
        if not conn:
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
            
            # # Convert topic to SQL pattern for wildcard matching
            # topic_pattern, is_wildcard = self._topic_to_sql_pattern(request.topic)
            # Generate the dynamic topic clause and parameter
            topic_clause, topic_param, _ = self._topic_clause(request.topic)
            
            # Build query with pagination
            # The page_token is a Unix timestamp (string) to continue from
            if page_token:
                try:
                    start_from = datetime.fromtimestamp(float(page_token), tz=timezone.utc)
                    time_filter = "timestamp > %s AND timestamp <= %s"
                    params = (start_from, end_time)
                except (ValueError, OSError):
                    context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                    context.set_details("Invalid page token")
                    return service_pb2.GetDataRangeResponse()
            else:
                time_filter = "timestamp >= %s AND timestamp <= %s"
                params = (start_time, end_time)
            
            # # Get total count for this query to check if there are more pages
            # if is_wildcard:
            #     count_query = f"""
            #         SELECT COUNT(*) FROM kv_data WHERE topic LIKE %s AND {time_filter}
            #     """
            # else:
            #     count_query = f"""
            #         SELECT COUNT(*) FROM kv_data WHERE topic = %s AND {time_filter}
            #     """
            
            # if is_wildcard:
            #     data_query = f"""
            #         SELECT timestamp, topic, payload
            #         FROM kv_data
            #         WHERE topic LIKE %s AND {time_filter}
            #         ORDER BY timestamp ASC
            #         LIMIT %s
            #     """
            # else:
            #     data_query = f"""
            #         SELECT timestamp, topic, payload
            #         FROM kv_data
            #         WHERE topic = %s AND {time_filter}
            #         ORDER BY timestamp ASC
            #         LIMIT %s
            #     """
            
            # Query structures built using the generated regex operator text
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

            # Execute queries
            # if is_wildcard:
            #     cursor.execute(count_query, (topic_pattern, *params))
            # else:
            #     cursor.execute(count_query, (request.topic, *params))
            # total_count = cursor.fetchone()[0]
            
            # if is_wildcard:
            #     cursor.execute(data_query, (topic_pattern, *params, page_size))
            # else:
            #     cursor.execute(data_query, (request.topic, *params, page_size))
            
            cursor.execute(count_query, (topic_param, *params))
            # total_count = cursor.fetchone()[0]
            count_result = cursor.fetchone()
            total_count = count_result[0] if count_result else 0
            
            cursor.execute(data_query, (topic_param, *params, page_size))

            rows = cursor.fetchall()
            
            # Convert to protobuf messages
            datapoints = [self._datapoint_to_proto(row) for row in rows]
            
            # Generate next page token if there are more results
            next_page_token = ""
            if len(rows) == page_size and total_count > len(rows):
                # Use the timestamp of the last result as the page token
                last_timestamp = rows[-1][0]
                next_page_token = str(last_timestamp.timestamp())
            
            return service_pb2.GetDataRangeResponse(
                datapoints=datapoints,
                next_page_token=next_page_token
            )
            
        except psycopg2.Error as e:
            self.logger.error(f"Database error: {e}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return service_pb2.GetDataRangeResponse()
        finally:
            conn.close()
    
    def StreamData(self, request, context):
        """Stream datapoints in real-time."""
        """Supporting wildcard topic parsing using regex."""
        conn = self._get_connection()
        if not conn:
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
                # start_from = cursor.fetchone()[0]
                time_result = cursor.fetchone()
                start_from = time_result[0] if time_result else datetime.now(timezone.utc)
            
            # Start streaming new data
            
            # self.logger.info(f"Starting stream for topic: {request.topic} from {start_from}")

            topic_clause, topic_param, _ = self._topic_clause(request.topic)
            self.logger.info(f"Starting stream for topic filter: {request.topic} from {start_from}")
            
            # while not context.is_active():
            # client needs to be active while running the queries, otherwise loop evaluates to false and exists without streaming any data.
            while context.is_active():
                # Query for new data
                # query = """
                #     SELECT timestamp, topic, payload
                #     FROM kv_data
                #     WHERE topic = %s AND timestamp > %s
                #     ORDER BY timestamp ASC
                #     LIMIT 100
                # """
                query = f"""
                    SELECT timestamp, topic, payload
                    FROM kv_data
                    WHERE {topic_clause} AND timestamp > %s
                    ORDER BY timestamp ASC
                    LIMIT 100
                """
                
                # cursor.execute(query, (request.topic, start_from))
                cursor.execute(query, (topic_param, start_from))
                rows = cursor.fetchall()
                
                if not rows:
                    time.sleep(0.1)  # Sleep briefly before checking for new data
                    continue

                for row in rows:
                    datapoint = self._datapoint_to_proto(row)
                    yield service_pb2.StreamDataResponse(datapoint=datapoint)
                    start_from = row[0]  # Update start_from to latest timestamp
                
                # if context.is_active():
                #     # Sleep briefly before checking for new data
                #     time.sleep(0.1)
                    
        except psycopg2.Error as e:
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
            time.sleep(86400)  # Sleep for a day
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
    
    # Get port from config or use default
    port = config.get('dws', {}).get('port', 50051)
    
    serve(db_config, port, debug=args.verbose)


if __name__ == '__main__':
    main()