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
                "selfUpdate": getattr(adapter_cls, "SELF_UPDATE", False),
            }
        )

    return response


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
        
        if is_polling:
            try:
                polling_rate_hz = int(polling_rate_hz)
            except Exception:
                polling_rate_hz = 1
            if polling_rate_hz <= 0:
                polling_rate_hz = 1
        else:
            # Callback mode: hz irrelevant; keep safe placeholder
            polling_rate_hz = 1
        
        
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