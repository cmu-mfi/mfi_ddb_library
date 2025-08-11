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
  GET  /stream/{conn_id}            : Server-sent events stream (1Hz updates)
  POST /publish                     : One-time MQTT message publish

Key Features:
- Automatic adapter discovery via reflection
- Callback-first streaming with polling fallback  
- Comprehensive error handling and status tracking
- Real-time monitoring via Server-Sent Events
- Topic family support (kv, blob, historian)
"""

import pkgutil
import importlib
import inspect
import asyncio
import datetime
import logging
from contextlib import asynccontextmanager
from typing import Tuple, Dict, Any, Optional

from fastapi import (
    APIRouter,
    BackgroundTasks,
    HTTPException,
    UploadFile,
    File,
    Form,
    Path as FastAPIPath,
)
from fastapi.responses import StreamingResponse

from mfi_ddb.streamer import Streamer
from api.data_adapters.utils.loader import load_config
# Removed import from stream.py - functionality moved inline
from api.data_adapters.utils.validator import validate_config_for_adapter

logger = logging.getLogger(__name__)


class AdapterFactory:
    """
    Adapter Discovery and Management Factory
    
    Automatically discovers and manages all data adapter classes from the mfi_ddb.data_adapters
    package using reflection. Provides adapter-agnostic methods for instantiation and capability
    detection.
    """

    def __init__(self) -> None:
        """Initialize factory and discover all available adapters."""
        self.adapter_map = self._discover_adapters()

    def _discover_adapters(self) -> Dict[str, Any]:
        """
        Dynamically discover adapter classes via reflection.
        
        Scans mfi_ddb.data_adapters package for classes ending with 'DataAdapter'
        and maps module name (protocol key) to adapter class.
        
        Returns:
            Dict mapping protocol keys to adapter classes
        """
        import mfi_ddb.data_adapters as adapters_pkg

        discovered: Dict[str, Any] = {}
        for _, module_name, _ in pkgutil.iter_modules(adapters_pkg.__path__):
            if module_name.startswith("_") or module_name == "base":
                continue
            try:
                module = importlib.import_module(f"{adapters_pkg.__name__}.{module_name}")
                for class_name, class_obj in inspect.getmembers(module, inspect.isclass):
                    if class_obj.__module__ == module.__name__ and class_name.endswith("DataAdapter"):
                        discovered[module_name] = class_obj
            except ImportError as err:
                logger.warning("Could not import adapter module '%s': %s", module_name, err)
        return discovered

    def supports_callbacks(self, protocol_key: str, adapter_instance: Optional[Any] = None) -> bool:
        """
        Check if adapter supports callbacks based on CALLBACK_SUPPORTED flag.
        
        Args:
            protocol_key: Adapter type identifier
            adapter_instance: Adapter instance to check for CALLBACK_SUPPORTED flag
            
        Returns:
            True if adapter has CALLBACK_SUPPORTED=True, False otherwise
        """
        if not adapter_instance:
            return False
            
        adapter_class = adapter_instance.__class__
        if hasattr(adapter_class, 'CALLBACK_SUPPORTED'):
            return bool(adapter_class.CALLBACK_SUPPORTED)
        
        return False

    def disconnect_adapter(self, protocol_key: str, adapter_instance: Any) -> bool:
        """
        Safely disconnect adapter and cleanup resources.
        
        Args:
            protocol_key: Adapter type identifier
            adapter_instance: Adapter instance to disconnect
            
        Returns:
            True if disconnect successful, False otherwise
        """
        try:
            if hasattr(adapter_instance, "disconnect"):
                adapter_instance.disconnect()
            return True
        except Exception as err:
            logger.error("Error disconnecting '%s' adapter: %s", protocol_key, err)
            return False


# FastAPI router and adapter factory initialization
router = APIRouter()
factory = AdapterFactory()
ADAPTER_MAP = factory.adapter_map

# Global connection state management
# These dictionaries maintain the complete state of all active connections
active_configs: Dict[str, Tuple[dict, dict, str]] = {}     # Connection configs: conn_id -> (full_config, inner_config, adapter_key)
active_adapters: Dict[str, Any] = {}                       # Adapter instances: conn_id -> adapter instance
active_streamers: Dict[str, Streamer] = {}                 # MQTT streamers: conn_id -> streamer instance
stream_rates: Dict[str, int] = {}                          # Polling rates: conn_id -> Hz
stop_flags: Dict[str, bool] = {}                           # Stop signals: conn_id -> stop requested
background_tasks_map: Dict[str, asyncio.Task] = {}         # Background tasks: conn_id -> polling task
connection_errors: Dict[str, str] = {}                     # Error tracking: conn_id -> last error message
last_successful_poll_iso: Dict[str, str] = {}              # Poll tracking: conn_id -> ISO timestamp
pause_flags: Dict[str, bool] = {}                          # Pause state: conn_id -> paused flag
actual_streaming_modes: Dict[str, str] = {}                # Actual mode used: conn_id -> "callback"|"polling"|"adapter-only"

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

def format_adapter_metadata(adapter_cls, protocol_key):
    """
    Extract and format adapter metadata for UI consumption.
    
    Collects all relevant metadata from adapter class attributes including
    name, configuration examples, help text, schema, and topic family.
    
    Args:
        adapter_cls: Adapter class with metadata attributes
        protocol_key: Protocol identifier (module name)
        
    Returns:
        Dictionary with formatted adapter metadata for API response
    """
    import yaml

    name = getattr(adapter_cls, "NAME", protocol_key)
    config_example = getattr(adapter_cls, "CONFIG_EXAMPLE", {})
    config_help = getattr(adapter_cls, "CONFIG_HELP", {})
    recommended_topic_family = getattr(adapter_cls, "RECOMMENDED_TOPIC_FAMILY", "historian")

    # YAML example
    example_yaml = ""
    if config_example:
        example_yaml = yaml.dump(config_example, default_flow_style=False, sort_keys=False)

    # Help (object + plain text)
    formatted_help_text = stringify_config_help(config_help)

    # JSON schema (if provided)
    schema = None
    schema_cls = getattr(adapter_cls, "SCHEMA", None)
    if schema_cls:
        try:
            if hasattr(schema_cls, "model_json_schema"):
                schema = schema_cls.model_json_schema()
            elif hasattr(schema_cls, "schema"):
                schema = schema_cls.schema()
        except Exception as e:
            logger.warning("Could not get schema for %s: %s", protocol_key, e)

    return {
        "key": protocol_key,
        "name": name,
        "configHelpText": formatted_help_text,
        "configExample": {"configuration": example_yaml, "raw": config_example},
        "recommendedTopicFamily": recommended_topic_family,
        "configSchema": schema,
        "hasSchema": schema is not None,
    }


def _decode_bytes(obj: Any) -> Any:
    """Recursively decode bytes to utf-8 in nested structures."""
    if isinstance(obj, bytes):
        return obj.decode("utf-8", errors="ignore")
    if isinstance(obj, dict):
        return {k: _decode_bytes(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_bytes(v) for v in obj]
    return obj


def _clear_connection_state(conn_id: str) -> None:
    """Drop all state for a connection id."""
    stream_rates.pop(conn_id, None)
    active_configs.pop(conn_id, None)
    connection_errors.pop(conn_id, None)
    last_successful_poll_iso.pop(conn_id, None)
    stop_flags.pop(conn_id, None)
    pause_flags.pop(conn_id, None)
    actual_streaming_modes.pop(conn_id, None)


async def _polling_loop(conn_id: str, streamer: Streamer, rate_hz: int) -> None:
    """Polling loop for adapters without callbacks."""
    failure_count = 0
    try:
        rate_float = float(rate_hz) if rate_hz else 1.0
        interval = 1.0 / rate_float
    except (ValueError, ZeroDivisionError):
        rate_float = 1.0
        interval = 1.0

    while not stop_flags.get(conn_id, False):
        try:
            await asyncio.get_running_loop().run_in_executor(
                None, streamer.poll_and_stream_data, int(rate_float)
            )
            failure_count = 0
            last_successful_poll_iso[conn_id] = datetime.datetime.now().isoformat()
            connection_errors.pop(conn_id, None)
        except Exception as err:
            msg = str(err).lower()
            if any(x in msg for x in ["connection refused", "host unreachable", "timeout", "no route"]):
                connection_errors[conn_id] = f"Broker unreachable: {err}"
            elif any(x in msg for x in ["authentication", "credential", "unauthorized"]):
                connection_errors[conn_id] = f"Broker auth failed: {err}"
            elif any(x in msg for x in ["disconnected", "connection lost", "not connected"]):
                connection_errors[conn_id] = f"Broker disconnected: {err}"
            elif "float" in msg and "integer" in msg:
                connection_errors[conn_id] = "Config error: Invalid stream rate"
            else:
                connection_errors[conn_id] = f"Streaming error: {err}"

            failure_count += 1
            logger.warning("Polling error (%s) failure %d: %s", conn_id, failure_count, err)

            if failure_count >= 3:
                connection_errors[conn_id] = f"Streaming stopped after {failure_count} failures: {err}"
                logger.error("Max failures for %s, stopping polling.", conn_id)
                break

            await asyncio.sleep(5)
        await asyncio.sleep(interval)

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
        if key in ADAPTER_MAP:
            adapter_key = key
            adapter_class = ADAPTER_MAP[key]
            break

    if not adapter_key:
        best_match = None
        min_errors = float("inf")
        validation_results = {}
        for potential_key, potential_class in ADAPTER_MAP.items():
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


@router.get("/adapters")
async def list_adapters() -> list[dict]:
    """
    List all discovered adapters with complete metadata.
    
    Dynamically discovers all adapter classes and returns their metadata including
    configuration examples, help text, validation schemas, and recommended topic families.
    Used by UI to populate adapter selection and configuration forms.
    
    Returns:
        List of adapter metadata dictionaries with keys:
        - key: Protocol identifier
        - name: Human-readable adapter name
        - configHelpText: Flattened help text for tooltips
        - configExample: YAML and raw configuration examples
        - recommendedTopicFamily: Suggested topic family (kv/blob/historian)
        - configSchema: JSON schema for validation
        - hasSchema: Boolean indicating schema availability
    """
    response: list[dict] = []
    for protocol_key, adapter_class in factory.adapter_map.items():
        if not hasattr(adapter_class, "NAME"):
            logger.warning("Adapter '%s' has no NAME attribute, using key", protocol_key)
        response.append(format_adapter_metadata(adapter_class, protocol_key))
    return response


@router.get("/health")
async def health_check() -> dict:
    """
    Service health check endpoint.
    
    Provides basic health status and metrics for monitoring and load balancing.
    Returns current timestamp and connection counts for operational visibility.
    
    Returns:
        Dictionary with health status, timestamp, and connection metrics
    """
    return {
        "status": "healthy",
        "timestamp": datetime.datetime.now().isoformat(),
        "active_connections": len(active_adapters),
        "streaming_connections": len(active_streamers),
    }


@router.post("/validate")
async def validate_endpoint(
    file: UploadFile = File(None),
    text: str = Form(None),
    topic_family: str = Form(None),
) -> dict:
    """
    Validate adapter configuration against schema.
    
    Accepts YAML configuration via file upload or text input, automatically detects
    the adapter type, and validates against the appropriate schema. Used by UI for
    real-time validation feedback during configuration editing.
    
    Args:
        file: Optional YAML configuration file upload
        text: Optional raw YAML configuration text
        topic_family: Optional topic family override (kv/blob/historian)
        
    Returns:
        Validation result with detected adapter, errors, and warnings
        
    Raises:
        HTTPException: 400 if validation fails or no configuration provided
    """
    try:
        config_dict = await load_config(file, text)
        if topic_family is not None:
            config_dict["topic_family"] = topic_family

        # Auto-detect adapter using the same logic as init_adapter_from_config
        common_fields = {"topic_family", "mqtt"}
        config_keys = set(config_dict.keys()) - common_fields
        
        # Try direct key matching first
        adapter_class = None
        detected_adapter = None
        
        for key in config_keys:
            if key in ADAPTER_MAP:
                adapter_class = ADAPTER_MAP[key]
                detected_adapter = key
                break
        
        # If no direct match, try validation-based detection
        if not adapter_class:
            best_match = None
            min_errors = float('inf')
            best_adapter_name = None
            
            for potential_key, potential_class in ADAPTER_MAP.items():
                validation_result = validate_config_for_adapter(config_dict, potential_class)
                error_count = len(validation_result.errors)
                
                if error_count == 0:
                    adapter_class = potential_class
                    detected_adapter = potential_key
                    break
                elif error_count < min_errors:
                    min_errors = error_count
                    best_match = potential_class
                    best_adapter_name = potential_key
            
            if not adapter_class and best_match:
                adapter_class = best_match
                detected_adapter = best_adapter_name

        if not adapter_class:
            raise HTTPException(400, "Could not determine adapter type from config structure")

        # Validate with detected adapter
        validation_result = validate_config_for_adapter(config_dict, adapter_class)
        
        # Add detected adapter info to response
        response = validation_result.to_dict()
        response["detected_adapter"] = detected_adapter
        
        return response
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(400, f"Validation failed: {str(e)}")
    

@router.post("/connect/{conn_id}")
async def connect_endpoint(
    conn_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(None),
    text: str = Form(None),
) -> dict:
    """
    Connect adapter and start data streaming.
    
    Creates adapter instance from configuration, establishes MQTT connection if configured,
    and starts streaming using callback-first approach with polling fallback. Maintains
    connection state for monitoring and management.
    
    Args:
        conn_id: Unique connection identifier
        background_tasks: FastAPI background tasks manager
        file: Optional YAML configuration file upload
        text: Optional raw YAML configuration text
        
    Returns:
        Connection result with streaming mode and broker status
        
    Raises:
        HTTPException: 502 if connection or streaming setup fails
    """
    try:
        if conn_id in active_configs:
            full_config, _, adapter_key = active_configs[conn_id]
            adapter_instance = active_adapters.get(conn_id)
            rate_hz = stream_rates.get(conn_id, 1)
            stop_flags[conn_id] = False
            pause_flags.pop(conn_id, None)
        else:
            full_config, adapter_key, adapter_instance, rate_hz = await init_adapter_from_config(conn_id, file, text)
    except Exception as err:
        logger.error("Connection failed: %s", err)
        raise HTTPException(status_code=502, detail=f"Connection failed: {err}")

    mqtt_cfg = full_config.get("mqtt")
    topic_family = full_config.get("topic_family", "historian")

    if mqtt_cfg:
        try:
            connection_errors.pop(conn_id, None)

            streamer_config = {
                "topic_family": topic_family,
                "mqtt": mqtt_cfg if isinstance(mqtt_cfg, dict) else mqtt_cfg.model_dump(),
            }

            # Intelligently choose streaming mode based on adapter capabilities
            if factory.supports_callbacks(adapter_key, adapter_instance):
                # Try callback mode first - efficient for event-driven adapters
                try:
                    streamer = await asyncio.get_running_loop().run_in_executor(
                        None, lambda: Streamer(streamer_config, adapter_instance, stream_on_update=True)
                    )
                    active_streamers[conn_id] = streamer
                    actual_streaming_modes[conn_id] = "callback"
                    return {"connected": True, "streaming_to_broker": True, "mode": "callback"}
                    
                except Exception as callback_err:
                    logger.warning("Callback mode failed for %s, trying polling mode: %s", adapter_key, callback_err)
                    
            # Use polling mode (either as fallback or preferred mode)
            try:
                streamer = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: Streamer(streamer_config, adapter_instance, stream_on_update=False)
                )
                active_streamers[conn_id] = streamer
                
                # Start background polling task for periodic data collection
                task = asyncio.create_task(_polling_loop(conn_id, streamer, rate_hz))
                background_tasks_map[conn_id] = task
                
                actual_streaming_modes[conn_id] = "polling"
                return {"connected": True, "streaming_to_broker": True, "mode": "polling"}
                
            except Exception as polling_err:
                logger.warning("Polling mode failed for %s, attempting without MQTT streaming: %s", adapter_key, polling_err)
                
                # Both callback and polling failed - likely MQTT broker issue
                # Connect adapter but without MQTT streaming (adapter-only mode)
                connection_errors.pop(conn_id, None)
                actual_streaming_modes[conn_id] = "adapter-only"
                return {"connected": True, "streaming_to_broker": False, "mode": "adapter-only", 
                       "note": "MQTT broker unreachable, adapter connected without streaming"}

        except Exception as err:
            logger.error("MQTT streaming setup failed: %s", err)
            _clear_connection_state(conn_id)
            message = str(err).lower()
            if "timeout" in message:
                connection_errors[conn_id] = f"MQTT broker unreachable: {err}"
            elif "hostname" in message or "getaddrinfo" in message:
                connection_errors[conn_id] = "MQTT broker unreachable: Cannot resolve hostname"
            else:
                connection_errors[conn_id] = f"MQTT connection failed: {err}"
            raise HTTPException(status_code=502, detail=connection_errors[conn_id])

    return {"connected": True, "streaming_to_broker": False, "note": "No MQTT configured"}


@router.post("/resume/{conn_id}")
async def resume_endpoint(
    conn_id: str = FastAPIPath(...),
    file: UploadFile = File(None),
    text: str = Form(None),
) -> dict:
    """Recreate adapter from config and restart streaming."""
    if not file and not text:
        raise HTTPException(status_code=400, detail="Configuration data required to resume connection.")

    # Stop existing streaming/polling
    if conn_id in active_adapters:
        stop_flags[conn_id] = True
        task = background_tasks_map.pop(conn_id, None)
        if task and not task.done():
            task.cancel()
        streamer = active_streamers.pop(conn_id, None)
        if streamer and hasattr(streamer, "disconnect"):
            try:
                streamer.disconnect()
            except Exception as err:
                logger.warning("Error disconnecting streamer: %s", err)

    try:
        full_config, adapter_key, adapter_instance, rate_hz = await init_adapter_from_config(conn_id, file, text)
    except Exception as err:
        _clear_connection_state(conn_id)
        raise HTTPException(status_code=502, detail=f"Failed to create adapter: {err}")

    mqtt_cfg = full_config.get("mqtt")
    topic_family = full_config.get("topic_family", "historian")

    if mqtt_cfg:
        try:
            connection_errors.pop(conn_id, None)

            streamer_config = {
                "topic_family": topic_family,
                "mqtt": mqtt_cfg if isinstance(mqtt_cfg, dict) else mqtt_cfg.model_dump(),
            }

            # Intelligently choose streaming mode based on adapter capabilities
            if factory.supports_callbacks(adapter_key, adapter_instance):
                # Try callback mode first - efficient for event-driven adapters
                try:
                    streamer = await asyncio.get_running_loop().run_in_executor(
                        None, lambda: Streamer(streamer_config, adapter_instance, stream_on_update=True)
                    )
                    active_streamers[conn_id] = streamer
                    actual_streaming_modes[conn_id] = "callback"
                    
                    return {
                        "resumed": True,
                        "connection_id": conn_id,
                        "streaming_to_broker": True,
                        "mode": "callback",
                    }
                    
                except Exception as callback_err:
                    logger.warning("Resume: Callback mode failed for %s, trying polling mode: %s", adapter_key, callback_err)
                    
            # Use polling mode (either as fallback or preferred mode)
            try:
                streamer = await asyncio.get_running_loop().run_in_executor(
                    None, lambda: Streamer(streamer_config, adapter_instance, stream_on_update=False)
                )
                active_streamers[conn_id] = streamer
                
                # Start background polling task
                task = asyncio.create_task(_polling_loop(conn_id, streamer, rate_hz))
                background_tasks_map[conn_id] = task
                
                actual_streaming_modes[conn_id] = "polling"
                return {
                    "resumed": True,
                    "connection_id": conn_id,
                    "streaming_to_broker": True,
                    "mode": "polling",
                }
                
            except Exception as polling_err:
                logger.warning("Resume: Polling mode failed for %s, attempting without MQTT streaming: %s", adapter_key, polling_err)
                
                # Both modes failed - likely MQTT broker issue
                # Resume adapter but without MQTT streaming (adapter-only mode)
                connection_errors.pop(conn_id, None)
                actual_streaming_modes[conn_id] = "adapter-only"
                return {
                    "resumed": True,
                    "connection_id": conn_id,
                    "streaming_to_broker": False,
                    "mode": "adapter-only",
                    "note": "MQTT broker unreachable, adapter resumed without streaming"
                }
        except Exception as err:
            _clear_connection_state(conn_id)
            raise HTTPException(status_code=502, detail=f"Failed to resume connection: {err}")

    return {"resumed": True, "connection_id": conn_id, "streaming_to_broker": False, "note": "No MQTT configured"}


@router.post("/pause/{conn_id}")
async def pause_endpoint(conn_id: str = FastAPIPath(...)) -> dict:
    """Stop streaming/polling but keep adapter state."""
    if conn_id not in active_adapters:
        raise HTTPException(status_code=404, detail="Connection not found")

    pause_flags[conn_id] = True
    stop_flags[conn_id] = True

    task = background_tasks_map.get(conn_id)
    if task and not task.done():
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=2.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
        background_tasks_map.pop(conn_id, None)

    streamer = active_streamers.pop(conn_id, None)
    if streamer and hasattr(streamer, "disconnect"):
        try:
            streamer.disconnect()
        except Exception as err:
            logger.warning("Error disconnecting streamer: %s", err)

    connection_errors.pop(conn_id, None)
    return {"paused": True, "connection_id": conn_id}


@router.post("/disconnect/{conn_id}")
async def disconnect_endpoint(conn_id: str = FastAPIPath(...)) -> dict:
    """Fully disconnect and clear state."""
    stop_flags[conn_id] = True

    # Cancel background task
    task = background_tasks_map.pop(conn_id, None)
    if task:
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass

    # Let streamer handle client + adapter
    streamer = active_streamers.pop(conn_id, None)
    if streamer and hasattr(streamer, "disconnect"):
        try:
            await asyncio.get_running_loop().run_in_executor(None, streamer.disconnect)
        except Exception as err:
            logger.error("Streamer disconnect failed: %s", err)

    active_adapters.pop(conn_id, None)
    _clear_connection_state(conn_id)

    return {"disconnected": True}


@router.get("/streaming-status/{conn_id}")
async def streaming_status_endpoint(conn_id: str = FastAPIPath(...)) -> dict:
    """Structured status for UI polling."""
    if conn_id not in active_configs:
        return {
            "status": "not_found",
            "error": "Connection not found - call /connect first",
            "adapter_connected": False,
            "is_streaming": False,
            "is_paused": False,
        }

    _, _, protocol_key = active_configs[conn_id]
    adapter_instance = active_adapters.get(conn_id)

    adapter_connected = conn_id in active_adapters
    is_streaming = conn_id in active_streamers
    explicitly_paused = pause_flags.get(conn_id, False)
    is_paused = explicitly_paused or (adapter_connected and not is_streaming)

    # Use the actual streaming mode that was established during connection
    streaming_mode = actual_streaming_modes.get(conn_id)
    if not streaming_mode and adapter_instance:
        # Fallback to detection if no mode recorded (shouldn't happen with new connections)
        streaming_mode = "callback" if factory.supports_callbacks(protocol_key, adapter_instance) else "polling"

    last_poll_iso = last_successful_poll_iso.get(conn_id)
    recent_success = False
    if last_poll_iso:
        try:
            last_dt = datetime.datetime.fromisoformat(last_poll_iso)
            recent_success = (datetime.datetime.now() - last_dt).total_seconds() < 60
        except Exception:
            recent_success = False

    task_state = "not_applicable"
    task_healthy = True
    if conn_id in background_tasks_map:
        task = background_tasks_map[conn_id]
        if task.done():
            if task.cancelled():
                task_state = "cancelled"
                task_healthy = False
            else:
                try:
                    task.result()
                    task_state = "completed"
                    task_healthy = False
                except Exception as err:
                    task_state = f"failed: {err}"
                    task_healthy = False
        else:
            task_state = "running"
            task_healthy = True

    if not adapter_connected:
        status = "inactive"
        reason = "Adapter not connected"
    elif is_paused:
        status = "paused"
        reason = "Streaming paused by user"
    elif not is_streaming:
        status = "inactive"
        reason = "Streamer not initialized"
    elif streaming_mode == "callback" and is_streaming:
        if conn_id in connection_errors:
            status = "inactive"
            reason = connection_errors[conn_id]
        else:
            status = "active"
            reason = "Streaming via callbacks"
    elif streaming_mode == "polling":
        if not task_healthy:
            status = "inactive"
            reason = f"Background polling task {task_state}"
        elif conn_id in connection_errors and not recent_success:
            status = "inactive"
            reason = connection_errors[conn_id]
        elif recent_success:
            status = "active"
            reason = "Streaming via polling"
        else:
            status = "starting"
            reason = "Polling task running, waiting for first success"
    else:
        status = "unknown"
        reason = "Unable to determine status"

    return {
        "status": status,
        "reason": reason,
        "adapter_connected": adapter_connected,
        "protocol": protocol_key,
        "streaming_mode": streaming_mode,
        "is_streaming": is_streaming,
        "is_paused": is_paused,
        "is_explicitly_paused": explicitly_paused,
        "background_task_status": task_state,
        "task_healthy": task_healthy,
        "recent_success": recent_success,
        "stream_rate": stream_rates.get(conn_id),
        "last_successful_poll": last_successful_poll_iso.get(conn_id),
        "connection_error": connection_errors.get(conn_id),
        "timestamp": datetime.datetime.now().isoformat(),
    }


@router.get("/stream/{conn_id}")
async def sse_stream_endpoint(conn_id: str = FastAPIPath(...)) -> StreamingResponse:
    """SSE stream: tiny status payload every second."""
    import json

    if conn_id not in active_adapters:
        raise HTTPException(status_code=404, detail="Connection not found")

    async def status_stream():
        counter = 0
        while True:
            try:
                status_data = await streaming_status_endpoint(conn_id)
                payload = {
                    "status": status_data.get("status", "unknown"),
                    "reason": status_data.get("reason", "No reason"),
                    "streaming_mode": status_data.get("streaming_mode", "unknown"),
                    "is_paused": status_data.get("is_paused", False),
                    "timestamp": datetime.datetime.now().isoformat(),
                    "counter": counter,
                }
                yield f"data: {json.dumps(payload)}\n\n"
                counter += 1
            except Exception as err:
                error_payload = {
                    "error": str(err),
                    "timestamp": datetime.datetime.now().isoformat(),
                    "counter": counter,
                }
                yield f"data: {json.dumps(error_payload)}\n\n"
                counter += 1
            await asyncio.sleep(1)

    return StreamingResponse(status_stream(), media_type="text/event-stream")


@router.post("/publish")
async def publish_endpoint(background_tasks: BackgroundTasks, id: str = Form(...)) -> dict:
    """
    One-time MQTT publish using existing adapter connection.
    
    Captures current data snapshot from active adapter and publishes it once
    to MQTT broker. Uses existing adapter instance for adapter-agnostic operation.
    
    Args:
        id: Connection ID of active adapter
        
    Returns:
        Success confirmation dictionary
        
    Raises:
        HTTPException: 400 if connection not found or MQTT not configured
    """
    # Validate connection exists and has MQTT configured
    if id not in active_configs:
        raise HTTPException(status_code=400, detail="No such connection ID")
    
    full_config, _, _ = active_configs[id]
    if "mqtt" not in full_config:
        raise HTTPException(status_code=400, detail="Cannot publish without MQTT block")
        
    # Get existing adapter instance (adapter-agnostic)
    adapter_instance = active_adapters.get(id)
    if not adapter_instance:
        raise HTTPException(status_code=400, detail="Adapter not active for this connection")

    async def publish_snapshot():
        """Background task to capture and publish data snapshot."""
        try:
            # Capture current data snapshot from any adapter type
            adapter_instance.get_data()  # Refresh data from source
            snapshot = getattr(adapter_instance, '_data', {})
            
            if not snapshot:
                return  # No data to publish
            
            # Create streamer for one-time publish (adapter-agnostic)
            streamer_config = {
                "topic_family": full_config.get("topic_family", "historian"),
                "mqtt": full_config["mqtt"],
            }
            
            # Publish using existing streamer infrastructure
            streamer = Streamer(streamer_config, adapter_instance, stream_on_update=False)
            
            # Publish each data component
            for component_id, data_payload in snapshot.items():
                if data_payload:  # Only publish non-empty data
                    streamer._publish(component_id, data_payload)
                    
        except Exception as e:
            logger.error(f"One-time publish failed for {id}: {e}")

    # Execute publish in background
    background_tasks.add_task(publish_snapshot)
    return {"published": True, "connection_id": id}


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
