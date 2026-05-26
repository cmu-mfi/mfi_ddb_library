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
from typing import Optional

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
                user=self.db_config.get('user', 'postgres'),
                password=self.db_config.get('password', '')
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
            datapoint.json_value = payload
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
            
            # Get exact match or closest past/future based on request
            if request.do_closest_past:
                # Get closest datapoint at or before the requested timestamp
                query = """
                    SELECT timestamp, topic, payload
                    FROM kv_data
                    WHERE topic = %s AND timestamp <= %s
                    ORDER BY timestamp DESC
                    LIMIT 1
                """
            else:
                # Get closest datapoint at or after the requested timestamp
                query = """
                    SELECT timestamp, topic, payload
                    FROM kv_data
                    WHERE topic = %s AND timestamp >= %s
                    ORDER BY timestamp ASC
                    LIMIT 1
                """
            
            cursor.execute(query, (request.topic, target_time))
            row = cursor.fetchone()
            
            if row:
                datapoint = self._datapoint_to_proto(row)
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
            
            # Get total count for this query to check if there are more pages
            count_query = f"""
                SELECT COUNT(*) FROM kv_data WHERE topic = %s AND {time_filter}
            """
            
            data_query = f"""
                SELECT timestamp, topic, payload
                FROM kv_data
                WHERE topic = %s AND {time_filter}
                ORDER BY timestamp ASC
                LIMIT %s
            """
            
            # Execute queries
            cursor.execute(count_query, (request.topic, *params))
            total_count = cursor.fetchone()[0]
            
            cursor.execute(data_query, (request.topic, *params, page_size))
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
                start_from = cursor.fetchone()[0]
            
            # Start streaming new data
            self.logger.info(f"Starting stream for topic: {request.topic} from {start_from}")
            
            while not context.is_active():
                # Query for new data
                query = """
                    SELECT timestamp, topic, payload
                    FROM kv_data
                    WHERE topic = %s AND timestamp > %s
                    ORDER BY timestamp ASC
                    LIMIT 100
                """
                
                cursor.execute(query, (request.topic, start_from))
                rows = cursor.fetchall()
                
                for row in rows:
                    datapoint = self._datapoint_to_proto(row)
                    yield service_pb2.StreamDataResponse(datapoint=datapoint)
                    start_from = row[0]  # Update start_from to latest timestamp
                
                if context.is_active():
                    # Sleep briefly before checking for new data
                    time.sleep(0.1)
                    
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