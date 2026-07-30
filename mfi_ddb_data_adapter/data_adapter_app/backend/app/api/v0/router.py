"""
Configuration Router for DDB Unified API

This module provides adapter-agnostic endpoints for managing industrial IoT data adapters
with real-time streaming capabilities using MQTT Sparkplug B protocol.
"""

import asyncio
import datetime
import importlib
import inspect
import logging
import pkgutil
import time
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional, Tuple, Iterable

import app.utils.utils as utils
import yaml
from app.services.adapter_factory import AdapterFactory
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi import Path as FastAPIPath

# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
from opentelemetry.metrics import get_meter, Observation

# Use the same meter namespace defined in main.py
meter = get_meter("mfi.data_adapter.app")

adapter_lifecycle_counter = meter.create_counter(
    "mfi_adapter_lifecycle_events_total",
    description="Total data adapter lifecycle events (connect, disconnect, pause, resume)"
)

adapter_validation_counter = meter.create_counter(
    "mfi_adapter_validation_total",
    description="Total configuration validation attempts"
)

# Active connections map
active_connections: Dict[str, AdapterFactory] = {}

# Asynchronous gauges to expose dynamic connection state to Prometheus in real-time
def get_active_connections_count(options) -> Iterable[Observation]:
    yield Observation(len(active_connections))

def get_active_streamers_count(options) -> Iterable[Observation]:
    active_streamers = sum(1 for conn in active_connections.values() if conn.is_streaming)
    yield Observation(active_streamers)

meter.create_observable_gauge(
    "mfi_adapter_active_connections",
    callbacks=[get_active_connections_count],
    description="Number of configured connection adapter instances"
)

meter.create_observable_gauge(
    "mfi_adapter_active_streamers",
    callbacks=[get_active_streamers_count],
    description="Number of active adapters currently streaming live data"
)

# to log the time of the connect request at that moment
adapter_last_connect_time = meter.create_gauge(
    "mfi_adapter_last_connect_timestamp_seconds",
    description="Timestamp of the last connect request in Unix seconds",
    unit="s"
)
# ==========================================

logger = logging.getLogger(__name__)

# FastAPI router initialization
router = APIRouter()


@router.get("/__debug__/connections")
async def debug_list_connections() -> Dict:
    """Returns the current in-process connection IDs for this worker/process."""
    logger.warning("Debug endpoint called; returning active_connections keys")
    return {"active_connections": list(active_connections.keys())}


@router.get("/adapters")
async def list_adapters() -> List[Dict]:
    """List all discovered adapters with complete metadata."""
    response: list[dict] = []
    data_adapters = AdapterFactory.discover_adapters()
    for adapter_name, adapter_cls in data_adapters.items():
        config_example = getattr(adapter_cls, "CONFIG_EXAMPLE", {})
        config_help = getattr(adapter_cls, "CONFIG_HELP", {})
        recommended_topic_family = getattr(
            adapter_cls, "RECOMMENDED_TOPIC_FAMILY", "historian"
        )

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
    """Service health check endpoint."""
    active_streamers = sum(1 for conn in active_connections.values() if conn.is_streaming)
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
    """Validate adapter configuration against schema."""
    try:
        config_dict = utils.load_config(file, text)
        temporary_instance = AdapterFactory(adp_name=adapter_name, adp_cfg=config_dict)
        is_valid = temporary_instance.validate_data_adapter_config()

        status_label = "VALID" if is_valid else "INVALID"
        adapter_validation_counter.add(1, {"target": "adapter", "adapter_name": adapter_name, "status": status_label})
        return {"is_valid": is_valid}

    except Exception as e:
        adapter_validation_counter.add(1, {"target": "adapter", "adapter_name": adapter_name, "status": "ERROR"})
        raise HTTPException(400, f"Validation failed: {str(e)}")


@router.post("/validate/streamer")
async def validate_streamer(
    file: UploadFile = File(None),
    text: str = Form(None),
) -> Dict:
    """Validate streamer configuration against schema."""
    try:
        config_dict = utils.load_config(file, text)
        temporary_instance = AdapterFactory(streamer_cfg=config_dict)
        is_valid = temporary_instance.validate_streamer_config()

        status_label = "VALID" if is_valid else "INVALID"
        adapter_validation_counter.add(1, {"target": "streamer", "status": status_label})
        return {"is_valid": is_valid}

    except Exception as e:
        adapter_validation_counter.add(1, {"target": "streamer", "status": "ERROR"})
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

    # log the time when the connect request is received, for Prometheus metrics
    adapter_last_connect_time.set(
        time.time(), 
        {"adapter_name": adapter_name, "conn_id": conn_id}
    )

    formatted_time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "+00"
    logger.info("Connect hit at %s for conn_id=%s, adapter=%s", formatted_time, conn_id, adapter_name)

    """Connect adapter and start data streaming."""
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
            logger.info("Stored connection: conn_id=%s keys_now=%s", conn_id, list(active_connections.keys()))
            
            adapter_lifecycle_counter.add(1, {"event": "connect", "adapter_name": adapter_name, "status": "SUCCESS"})
        except Exception as err:
            logger.exception("connect_and_stream failed for conn_id=%s", conn_id)
            adapter_lifecycle_counter.add(1, {"event": "connect", "adapter_name": adapter_name, "status": "ERROR"})
            raise HTTPException(status_code=502, detail=f"Connection failed: {err}")

    elif not connection.is_streaming:
        try:
            connection.resume_streaming()
            adapter_lifecycle_counter.add(1, {"event": "resume", "adapter_name": adapter_name, "status": "SUCCESS"})
        except Exception as err:
            adapter_lifecycle_counter.add(1, {"event": "resume", "adapter_name": adapter_name, "status": "ERROR"})
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
    adapter_name = connection.adp_name
    try:
        connection.resume_streaming()
        adapter_lifecycle_counter.add(1, {"event": "resume", "adapter_name": adapter_name, "status": "SUCCESS"})
    except Exception as err:
        adapter_lifecycle_counter.add(1, {"event": "resume", "adapter_name": adapter_name, "status": "ERROR"})
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
    adapter_name = connection.adp_name
    try:
        connection.pause_streaming()
        adapter_lifecycle_counter.add(1, {"event": "pause", "adapter_name": adapter_name, "status": "SUCCESS"})
    except Exception as err:
        adapter_lifecycle_counter.add(1, {"event": "pause", "adapter_name": adapter_name, "status": "ERROR"})
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
    adapter_name = connection.adp_name   
    try:
        connection.disconnect()
        
        if connection.is_connected:
            adapter_lifecycle_counter.add(1, {"event": "disconnect", "adapter_name": adapter_name, "status": "ERROR"})
            raise HTTPException(status_code=502, detail="Disconnection failed: still connected")
        else:
            del active_connections[conn_id]
            adapter_lifecycle_counter.add(1, {"event": "disconnect", "adapter_name": adapter_name, "status": "SUCCESS"})
            
    except Exception as err:
        adapter_lifecycle_counter.add(1, {"event": "disconnect", "adapter_name": adapter_name, "status": "ERROR"})
        raise HTTPException(status_code=502, detail=f"Disconnection failed: {err}")
        
    return {"disconnected": True}


@router.get("/streaming-status/{conn_id}")
async def streaming_status_endpoint(conn_id: str = FastAPIPath(...)) -> dict:
    """Structured status for UI polling."""
    logger.debug(active_connections)
    if conn_id not in active_connections:
        logger.debug(f"active_connections.keys(): {list(active_connections.keys())}")
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]
    return {
        "adapter_name": connection.adp_name,
        "streaming_mode": "polling" if connection.is_polling else "callback",
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "timestamp": datetime.datetime.now().isoformat(),
    }