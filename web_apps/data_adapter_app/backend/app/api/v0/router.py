"""
Configuration Router for DDB Unified API

This module provides adapter-agnostic endpoints for managing industrial IoT data adapters
with real-time streaming capabilities using MQTT Sparkplug B protocol.

API Endpoints:
  GET  /adapters                    : List all available adapters with metadata
  GET  /health                      : Service health check
  POST /validate                    : Validate YAML configuration against adapter schema
  POST /connect/{conn_id}           : Connect adapter and start streaming
  POST /resume/{conn_id}            : Resume connection with new configuration
  POST /pause/{conn_id}             : Pause streaming (keep adapter instance)
  POST /disconnect/{conn_id}        : Disconnect and cleanup adapter
  GET  /streaming-status/{conn_id}  : Get real-time connection status

Key Features:
- Automatic adapter discovery via reflection @SA NO NEED. INPUT CONFIG DRIVEN
- Callback-first streaming with polling fallback @SA NOT NEEDED. DEFINED AS PER CONNECTION
- Comprehensive error handling and status tracking @SA REVIEWED
- Real-time monitoring via Server-Sent Events @SA TODO: DON'T UNDERSTAND
- Topic family support (kv, blob, historian) @SA NO NEED
"""

import asyncio
import datetime
import importlib
import inspect
import logging
import pkgutil
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple

import app.utils.utils as utils
import yaml
from app.services.adapter_factory import AdapterFactory
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi import Path as FastAPIPath

logger = logging.getLogger(__name__)

# FastAPI router and adapter factory initialization
router = APIRouter()

# Connection: conn_id -> AdapterFactory instance
active_connections: Dict[str, AdapterFactory] = {}

''' @SA POTENTIALLY NOT NEEDED. USE yaml.dump INSTEAD
def stringify_config_help(config_help: dict) -> str:
    """
    Convert nested configuration help to flat string format.
    
    Recursively flattens nested help dictionaries into a simple format
    suitable for UI tooltips and help text display.
    
    Args:
        config_help: Nested dictionary with field descriptions
        
    Returns:
        Multi-line string with field_name: description format
    """
    if not config_help:
        return ""
    lines = []
    def walker(obj):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, dict):
                    walker(v)
                else:
                    lines.append(f"{k}: {v}")
        else:
            lines.append(str(obj))
    walker(config_help)
    return "\n".join(lines)
'''

''' @SA NOT NEEDED. ONLY STATE VARIABLE USED IS active_connections
def _clear_connection_state(conn_id: str) -> None:
    """Drop all state for a connection id."""
    stream_rates.pop(conn_id, None)
    active_configs.pop(conn_id, None)
    connection_errors.pop(conn_id, None)
    last_successful_poll_iso.pop(conn_id, None)
    stop_flags.pop(conn_id, None)
    pause_flags.pop(conn_id, None)
    actual_streaming_modes.pop(conn_id, None)
'''

''' @SA POTENTIALLY NOT NEEDED. USE AdapterFactory objects INSTEAD
async def init_adapter_from_config(
    conn_id: str,
    file: Optional[UploadFile],
    text: Optional[str],
    *,
    instantiate_timeout: Optional[float] = None,
) -> Tuple[dict, str, Any, int]:
    """Load config, detect adapter, instantiate, and store state."""
    full_config: dict = await load_config(file, text)

    # Detect adapter
    adapter_key = None
    adapter_class = None
    common_fields = {"topic_family", "mqtt"}
    config_keys = set(full_config.keys()) - common_fields

    for key in config_keys:
        if key in factory.adapter_map:
            adapter_key = key
            adapter_class = factory.adapter_map[key]
            break

    if not adapter_key:
        best_match = None
        min_errors = float("inf")
        validation_results = {}
        for potential_key, potential_class in factory.adapter_map.items():
            try:
                validation_result = validate_config_for_adapter(full_config, potential_class)
                error_count = len(validation_result.errors)
                validation_results[potential_key] = error_count
                
                logger.info(f"Validation for {potential_key}: {error_count} errors")
                
                if error_count == 0:
                    adapter_key = potential_key
                    adapter_class = potential_class
                    logger.info(f"Perfect match found: {potential_key}")
                    break
                elif error_count < min_errors:
                    min_errors = error_count
                    best_match = (potential_key, potential_class)
            except Exception as e:
                logger.warning(f"Validation failed for {potential_key}: {e}")
                validation_results[potential_key] = float('inf')
        if not adapter_key and best_match:
            adapter_key, adapter_class = best_match

    if not adapter_key or not adapter_class:
        raise HTTPException(400, f"Could not determine adapter type from config keys: {list(config_keys)}")

    # Extract adapter-specific config (keyed or direct)
    if adapter_key in full_config:
        inner_config = full_config[adapter_key]
    else:
        inner_config = {k: v for k, v in full_config.items() if k not in common_fields}
    inner_config = _decode_bytes(inner_config)
    loop = asyncio.get_running_loop()

    async def _build_instance():
        try:
            return await loop.run_in_executor(None, lambda: adapter_class({adapter_key: inner_config}))
        except Exception as wrapped_err:
            try:
                return await loop.run_in_executor(None, lambda: adapter_class(inner_config))
            except Exception as inner_err:
                raise RuntimeError(
                    f"Failed to create '{adapter_key}' adapter. wrapped_error={wrapped_err} ; inner_error={inner_err}"
                )

    adapter_instance = (
        await _build_instance()
        if instantiate_timeout is None
        else await asyncio.wait_for(_build_instance(), timeout=instantiate_timeout)
    )

# Wait for adapter data to populate 
# ---------- This block handles adapters that need time to populate _data after instantiation.
    MAX_RETRIES = 30
    WAIT_SEC = 0.5

    logger.info(f"Waiting for adapter {adapter_key} to populate _data before starting Streamer...")

    for attempt in range(MAX_RETRIES):
        current_data = getattr(adapter_instance, "_data", {})
        
        # Check if data is populated (all values should be dicts with content)
        if current_data and all(isinstance(val, dict) and bool(val) for val in current_data.values()):
            logger.info(f"Adapter {adapter_key} data ready after {attempt + 1} attempts")
            break

        # Try to trigger data collection if adapter supports it
        if hasattr(adapter_instance, "get_data"):
            try:
                await loop.run_in_executor(None, adapter_instance.get_data)
            except Exception as e:
                logger.warning(f"get_data() failed on attempt {attempt + 1} for {adapter_key}: {e}")

        await asyncio.sleep(WAIT_SEC)
    else:
        # Log warning but don't fail - some adapters might work differently
        logger.warning(
            f"Adapter {adapter_key} did not populate data within {MAX_RETRIES * WAIT_SEC}s. "
            f"Proceeding anyway - adapter may populate data via callbacks or on first poll."
        )

    # ROS SPECIFIC: TO BE HANDLED
    # ------------ END SYNCHRONOUS ADAPTER COMPATIBILITY BLOCK

    active_configs[conn_id] = (full_config, inner_config, adapter_key)
    active_adapters[conn_id] = adapter_instance
    rate_hz = int(inner_config.get("stream_rate", 1))
    stream_rates[conn_id] = rate_hz
    stop_flags[conn_id] = False
    pause_flags.pop(conn_id, None)

    return full_config, adapter_key, adapter_instance, rate_hz
'''


# @SA REVIEWED AND EDITED
@router.get("/adapters")
async def list_adapters() -> List[Dict]:
    """
    List all discovered adapters with complete metadata.

    Dynamically discovers all adapter classes and returns their metadata including
    configuration examples, help text, validation schemas, and recommended topic families.
    Used by UI to populate adapter selection and configuration forms.

    Returns:
        List of adapter metadata dictionaries with keys:
        - key: Adapter class name
        - name: Human-readable adapter name
        - configHelpText: Flattened help text for tooltips
        - configExample: YAML and raw configuration examples
        - recommendedTopicFamily: Suggested topic family (kv/blob/historian)
        - configSchema: JSON schema for validation
    """
    response: list[dict] = []
    data_adapters = AdapterFactory.discover_adapters()
    for adapter_name, adapter_cls in data_adapters.items():
        config_example = getattr(adapter_cls, "CONFIG_EXAMPLE", {})
        config_help = getattr(adapter_cls, "CONFIG_HELP", {})
        recommended_topic_family = getattr(
            adapter_cls, "RECOMMENDED_TOPIC_FAMILY", "historian"
        )

        # YAML example
        example_yaml = ""
        if config_example:
            example_yaml = yaml.dump(
                config_example, default_flow_style=False, sort_keys=False
            )

        response.append(
            {
                "key": adapter_cls.__name__,
                "name": adapter_name,
                "configHelpText": yaml.dump(config_help),
                "configExample": {"configuration": example_yaml, "raw": config_example},
                "recommendedTopicFamily": recommended_topic_family,
                "configSchema": adapter_cls.SCHEMA.model_json_schema(),
            }
        )

    return response


# @SA REVIEWED. NO CHANGE
@router.get("/health")
async def health_check() -> Dict:
    """
    Service health check endpoint.

    Provides basic health status and metrics for monitoring and load balancing.
    Returns current timestamp and connection counts for operational visibility.

    Returns:
        Dictionary with health status, timestamp, and connection metrics
    """
    
    active_streamers = 0
    for connection in active_connections.values():
        if connection.is_streaming:
            active_streamers += 1
    
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat(),
        "active_connections": len(active_connections),
        "streaming_connections": active_streamers,
    }


# @SA REVIEWED AND EDITED
@router.post("/validate/adapter")
async def validate_adapter(
    adapter_name: str = Form(...),
    file: UploadFile = File(None),
    text: str = Form(None),
) -> Dict:
    """
    Validate adapter configuration against schema.

    Accepts YAML configuration via file upload or text input, automatically detects
    the adapter type, and validates against the appropriate schema. Used by UI for
    real-time validation feedback during configuration editing.

    Args:
        adapter_name: Adapter type identifier
        file: Optional YAML configuration file upload
        text: Optional raw YAML configuration text

    Returns:
        Dictionary with validation result:
        - is_valid: True if config is valid, False otherwise

    Raises:
        HTTPException: 400 if validation fails or no configuration provided
    """
    try:
        config_dict = utils.load_config(file, text)
        temporary_instance = AdapterFactory(adp_name=adapter_name, adp_cfg=config_dict)

        is_valid = temporary_instance.validate_data_adapter_config()

        return {"is_valid": is_valid}

    except Exception as e:
        raise HTTPException(400, f"Validation failed: {str(e)}")


# @SA ADDED
@router.post("/validate/streamer")
async def validate_streamer(
    file: UploadFile = File(None),
    text: str = Form(None),
) -> Dict:
    """
    Validate streamer configuration against schema.

    Accepts YAML configuration via file upload or text input, automatically detects
    the adapter type, and validates against the appropriate schema. Used by UI for
    real-time validation feedback during configuration editing.

    Args:
        file: Optional YAML configuration file upload
        text: Optional raw YAML configuration text

    Returns:
        Dictionary with validation result:
        - is_valid: True if config is valid, False otherwise

    Raises:
        HTTPException: 400 if validation fails or no configuration provided
    """
    try:
        config_dict = utils.load_config(file, text)
        temporary_instance = AdapterFactory(streamer_cfg=config_dict)

    
        is_valid = temporary_instance.validate_streamer_config()

        return {"is_valid": is_valid}

    except Exception as e:
        raise HTTPException(400, f"Validation failed: {str(e)}")


# @SA REVIEWED AND EDITED
@router.post("/connect/{conn_id}")
async def connect_endpoint(
    conn_id: str = FastAPIPath(...),
    adapter_name: str = Form(...),
    adapter_file: UploadFile = File(None),
    adapter_text: str = Form(None),
    streamer_file: UploadFile = File(None),
    streamer_text: str = Form(None),
    is_polling: bool = Form(True),
    polling_rate_hz: int = Form(1),
) -> dict:
    """
    Connect adapter and start data streaming.

    Creates adapter instance from configuration, establishes Streamer connection if configured,
    Maintains connection state for monitoring and management.

    Returns:
        Connection result with streaming mode and broker status

    Raises:
        HTTPException: 502 if connection or streaming setup fails
    """

    if conn_id in active_connections:
        connection = active_connections[conn_id]
        if connection.adp_name != adapter_name:
            raise HTTPException(
                status_code=400,
                detail=f"Connection ID '{conn_id}' already exists with a different adapter '{connection.adp_name}'.",
            )
    else:
        adapter_cfg = utils.load_config(adapter_file, adapter_text)
        streamer_cfg = utils.load_config(streamer_file, streamer_text)
        connection = AdapterFactory(
            adp_name=adapter_name,
            adp_cfg=adapter_cfg,
            streamer_cfg=streamer_cfg,
            is_polling=is_polling,
            polling_rate_hz=polling_rate_hz,
        )

    if not connection.is_connected:
        try:
            connection.connect_and_stream()
            active_connections[conn_id] = connection
        except Exception as err:
            raise HTTPException(status_code=502, detail=f"Connection failed: {err}")

    elif not connection.is_streaming:
        try:
            connection.resume_streaming()
        except Exception as err:
            raise HTTPException(
                status_code=502, detail=f"Streaming resume failed: {err}"
            )

    return {
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }


# @SA REVIEWED AND EDITED
@router.post("/resume/{conn_id}")
async def resume_endpoint(
    conn_id: str = FastAPIPath(...),
) -> dict:

    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    connection = active_connections[conn_id]
    try:
        connection.resume_streaming()
    except Exception as err:
        raise HTTPException(
            status_code=502, detail=f"Streaming resume failed: {err}"
        )
        
    return {
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }      


# @SA REVIEWED AND EDITED
@router.post("/pause/{conn_id}")
async def pause_endpoint(
    conn_id: str = FastAPIPath(...)
) -> dict:
    """Pause streaming but keep adapter instance alive."""
    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]
    try:
        connection.pause_streaming()
    except Exception as err:
        raise HTTPException(status_code=502, detail=f"Streaming pause failed: {err}")

    return {
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }


# @SA REVIEWED AND EDITED
@router.post("/disconnect/{conn_id}")
async def disconnect_endpoint(
    conn_id: str = FastAPIPath(...)
) -> dict:
    """Fully disconnect and clear state."""

    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    connection = active_connections[conn_id]    
    try:
        connection.disconnect()
    except Exception as err:
        raise HTTPException(status_code=502, detail=f"Disconnection failed: {err}")
    
    if connection.is_connected:
        raise HTTPException(status_code=502, detail="Disconnection failed: still connected")
    else:
        del active_connections[conn_id]
        
    return {"disconnected": True}


# @SA REVIEWED AND EDITED
@router.get("/streaming-status/{conn_id}")
async def streaming_status_endpoint(conn_id: str = FastAPIPath(...)) -> dict:
    """Structured status for UI polling."""
    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]

    return {
        "adapter_name": connection.adp_name,
        "streaming_mode": "polling" if connection.is_polling else "callback",
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "timestamp": datetime.datetime.now().isoformat(),
    }


# @SA NOT REQUIRED WITH ADAPTERFACTORY IMPLEMENTATION
'''
@asynccontextmanager
async def lifespan(app):
    """Startup/shutdown cleanup for background tasks, streamers, and adapters."""
    logger.info("Backend starting up")
    yield
    logger.info("Backend shutting down")

    for conn_id, task in list(background_tasks_map.items()):
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass

    for conn_id, streamer in list(active_streamers.items()):
        if hasattr(streamer, "disconnect"):
            try:
                streamer.disconnect()
            except Exception as err:
                logger.warning("Error disconnecting streamer %s: %s", conn_id, err)

    for conn_id, adapter_instance in list(active_adapters.items()):
        if conn_id in active_configs:
            _, _, protocol_key = active_configs[conn_id]
            factory.disconnect_adapter(protocol_key, adapter_instance)
'''