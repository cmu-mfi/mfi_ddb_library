#This module provides functions for creating a real-time data stream for web clients.

import time
import json
from mfi_ddb.streamer import Streamer
from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from mfi_ddb.data_adapters.mqtt import MqttDataAdapter
from mfi_ddb.data_adapters.local_files import LocalFilesDataAdapter
from mfi_ddb.data_adapters.ros import RosDataAdapter

# Protocol mapping dictionary - maps protocol strings to their corresponding adapter classes
# This allows dynamic adapter instantiation based on protocol type
PROTOCOL_MAP = {
    'mtconnect': MTconnectDataAdapter,  # Manufacturing protocol for machine tools
    'mqtt':      MqttDataAdapter,       # Message queuing telemetry transport
    'file':      LocalFilesDataAdapter, # Local file system monitoring
    'ros':       RosDataAdapter,        # Robot Operating System protocol
}


async def event_stream(adapter, rate):
    """
    Server-Sent Events (SSE) endpoint that observes data without interfering with MQTT streaming.
    
    This function creates a real-time data stream for web clients, providing:
    - Live monitoring of adapter data changes
    - Non-intrusive observation (read-only access to adapter data)
    - Structured JSON payloads with timestamps and status indicators
    
    Args:
        adapter: Data adapter instance (MTConnect, MQTT, LocalFiles, or ROS)
        rate: Streaming frequency in Hz (messages per second)
        
    Yields:
        str: SSE-formatted data strings containing JSON payloads
    """
    import asyncio
    
    # Initialize streaming state variables
    counter = 0                # Message sequence counter for client synchronization
    last_seen_data = {}       # Cache of previous data state for change detection


    # Send initial handshake message to establish connection with client
    initial = {
        "message": "Stream connection established",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),  # ISO 8601 timestamp
        "status": "CONNECTED",                              # Connection status indicator
        "counter": counter,                                 # Message sequence number
    }
    yield f"data: {json.dumps(initial)}\n\n"  # SSE format: "data: <json>\n\n"
    counter += 1

    # Main streaming loop - continues until client disconnects
    while True:
        try:
            # Non-destructive data access - peek at adapter's current state
            # Using .copy() to prevent accidental modification of adapter's internal data
            current_data = getattr(adapter, '_data', {}).copy()
            buffer_data = getattr(adapter, 'buffer_data', [])
            buffer_length = len(buffer_data)
            
            # Change detection logic - only stream when data actually changes
            data_changed = current_data != last_seen_data
            
            # Data quality check - ensure we have meaningful data to send
            # Filters out empty dictionaries and falsy values
            has_meaningful_data = any(
                isinstance(v, dict) and any(v.values()) if isinstance(v, dict) else bool(v)
                for v in current_data.values()
            )
            
            # Conditional payload generation based on data state
            if has_meaningful_data:
                # New meaningful data detected - send data update
                payload = {
                    "data": current_data,                           # Actual sensor/device data
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "DATA_UPDATE",                        # Indicates new data available
                    "counter": counter,
                }
                last_seen_data = current_data.copy()  # Update cache for next comparison
                print(f"[SSE] New data: {list(current_data.keys())}")  # Log data component keys
                
            elif buffer_length > 0:
                # Data in buffer but no changes - send monitoring status
                payload = {
                    "message": "Monitoring...",
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "MONITORING",                         # System is active but no new data
                    "counter": counter,
                    "buffer_length": 0,  # Hide internal buffer details from client
                }
            else:
                # No data changes, no buffer data - send basic heartbeat
                payload = {
                    "message": "Monitoring...",
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "MONITORING",
                    "counter": counter,
                }

            # Send payload to client in SSE format
            # default=str handles non-JSON-serializable objects (like datetime)
            yield f"data: {json.dumps(payload, default=str)}\n\n"
            counter += 1

        except Exception as e:
            # Error handling - send error message to client instead of crashing
            err = {
                "error": str(e),                               # Error description
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": "ERROR",                             # Error status indicator
                "counter": counter,
            }
            yield f"data: {json.dumps(err)}\n\n"
            counter += 1

        # Rate limiting - control streaming frequency
        # Sleep for 1/rate seconds (e.g., rate=10 → sleep 0.1s → 10 Hz)
        await asyncio.sleep(1 / rate)


async def publish_once(adapter_cfg: dict, topic_family: str):
    """
    One-shot publish endpoint: captures a single data snapshot and publishes it once.
    
    This function provides a "publish now" functionality as opposed to continuous streaming.
    Useful for:
    - Manual data collection triggers
    - Testing/debugging data adapters
    - On-demand data snapshots
    
    Args:
        adapter_cfg (dict): Complete adapter configuration dictionary
        topic_family (str): MQTT topic family classification (e.g., 'historian', 'kv', 'blob')
        
    Returns:
        None: Function publishes data and returns immediately
    """
    # Instantiate the MQTT adapter with the provided configuration
    # Note: Currently hardcoded to MqttDataAdapter - could be made dynamic using PROTOCOL_MAP
    adapter = MqttDataAdapter(adapter_cfg)

    # Trigger a single data collection cycle
    # This pulls fresh data from the configured data source
    adapter.get_data()
    
    # Extract the collected data snapshot
    # Uses getattr with default to handle cases where _data might not exist
    snapshot = getattr(adapter, '_data', {})

    # Initialize the MQTT publisher/streamer
    # stream_on_update=False ensures this is a one-time publish, not continuous streaming
    publisher = Streamer(
        {'topic_family': topic_family, 'mqtt': adapter_cfg['mqtt']},  # Publisher configuration
        adapter,                                                       # Data source adapter
        stream_on_update=False                                        # Disable automatic streaming
    )

    # Publish each data component as a separate MQTT message
    # This allows granular consumption of different data types by subscribers
    for comp, payload in snapshot.items():
        publisher._publish(comp, payload)  # Send component data to MQTT broker