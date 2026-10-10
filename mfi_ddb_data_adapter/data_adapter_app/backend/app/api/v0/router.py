"""
Configuration Router for DDB Unified API

This module provides adapter-agnostic endpoints for managing industrial IoT data adapters
with real-time streaming capabilities using MQTT Sparkplug B protocol.

API Endpoints:
  GET    /all               : List every connection this backend knows about, with config and live status
  GET    /adapters          : List all available adapters with metadata
  GET    /streamer          : Streamer configuration metadata (example, help text, schema)
  GET    /health            : Service health check
  POST   /validate/adapter  : Validate adapter YAML configuration against its schema
  POST   /validate/streamer : Validate streamer YAML configuration against its schema
  POST   /connect           : Create a brand new connection (backend generates its id)
  POST   /connect/{conn_id} : Reconnect an existing (e.g. stopped) connection
  POST   /update/{conn_id}  : Apply an updated configuration to an existing connection
  POST   /resume/{conn_id}  : Resume streaming on a paused connection
  POST   /pause/{conn_id}   : Pause streaming (polling mode only), keep adapter instance
  POST   /stop/{conn_id}    : Stop a connection but keep it known (for later reconnect)
  POST   /delete/{conn_id}  : Stop (if needed) and fully forget a connection

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
import os
import pkgutil
from contextlib import asynccontextmanager
from pathlib import Path as FilePath
from typing import Any, Dict, List, Optional, Tuple

import app.services.connection_store as connection_store
import app.utils.utils as utils
import mfi_ddb
import yaml
from app.services.adapter_factory import AdapterFactory
from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi import Path as FastAPIPath
from starlette.concurrency import run_in_threadpool

logger = logging.getLogger(__name__)

# FastAPI router and adapter factory initialization
router = APIRouter()

# Connection: conn_id -> AdapterFactory instance
active_connections: Dict[str, AdapterFactory] = {}

DB_PATH = FilePath(os.environ.get("MFI_DAA_DB_PATH", "data/connections.db"))


@asynccontextmanager
async def lifespan(_app):
    """
    On startup, load every connection known from the SQLite store, but do
    NOT reconnect/resume streaming automatically. Every loaded connection comes back
    known but stopped; the user has to explicitly reconnect it, so a
    restart is never silently invisible to them. 
    """
    connection_store.init_db(DB_PATH)
    for entry in connection_store.load_all_connections(DB_PATH):
        conn_id = entry["conn_id"]
        try:
            connection = AdapterFactory(
                adp_name=entry["adapter_name"],
                adp_cfg=entry["adapter_cfg"],
                streamer_cfg=entry["streamer_cfg"],
                is_polling=entry["is_polling"],
                polling_rate_hz=entry["polling_rate_hz"],
            )
            active_connections[conn_id] = connection
            if entry["desired_state"] != "stopped":
                connection_store.update_desired_state(conn_id, "stopped", db_path=DB_PATH)
            logger.info("Loaded connection conn_id=%s, stopped (previous desired_state=%s)", conn_id, entry["desired_state"])
        except Exception as err:
            logger.warning("Failed to load connection conn_id=%s, skipping: %s", conn_id, err)

    yield


@router.get("/all")
async def get_all() -> List[Dict]:
    """
    List every connection this backend knows about, with full configuration
    and live status - including ones that are currently stopped, not just
    ones actively streaming.

    Source of truth for the UI's connection list, replacing the previous
    localStorage-based cache: everything needed to render and reconstruct
    a connection (adapter type, its config, the streamer config, polling
    settings, and current connect/stream state) comes from here instead.

    Returns:
        List of dictionaries with keys:
        - id: Connection id
        - adapter: Adapter name (e.g. "MQTT", "Modbus")
        - adapterConfig: Adapter configuration as YAML text
        - streamerConfig: Streamer configuration as YAML text
        - isPolling: Whether the connection is in polling mode
        - pollingRateHz: Polling rate in Hz (meaningful only when isPolling)
        - isConnected: Whether the adapter is currently connected
        - isStreaming: Whether the connection is currently streaming
    """
    response: List[Dict] = []
    for conn_id, connection in active_connections.items():
        response.append(
            {
                "id": conn_id,
                "adapter": connection.adp_name,
                "adapterConfig": yaml.dump(
                    connection.adp_cfg, default_flow_style=False, sort_keys=False
                ),
                "streamerConfig": yaml.dump(
                    connection.streamer_cfg, default_flow_style=False, sort_keys=False
                ),
                "isPolling": connection.is_polling,
                "pollingRateHz": connection.polling_rate_hz,
                "isConnected": connection.is_connected,
                "isStreaming": connection.is_streaming,
            }
        )
    return response

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


@router.get("/streamer")
async def get_streamer_config() -> Dict:
    """
    Return Streamer configuration metadata (example, help text, and schema).

    Mirrors the /adapters endpoint's shape so the UI can populate the streamer
    configuration field from the same source of truth as the adapter fields,
    instead of hardcoding a default configuration in the frontend.

    Returns:
        Dictionary with keys:
        - configHelpText: Flattened help text for tooltips
        - configExample: YAML and raw configuration examples
        - configSchema: JSON schema for validation
    """
    config_example = getattr(mfi_ddb.Streamer, "CONFIG_EXAMPLE", {})
    config_help = getattr(mfi_ddb.Streamer, "CONFIG_HELP", {})

    example_yaml = ""
    if config_example:
        example_yaml = yaml.dump(
            config_example, default_flow_style=False, sort_keys=False
        )

    return {
        "configHelpText": yaml.dump(config_help),
        "configExample": {"configuration": example_yaml, "raw": config_example},
        "configSchema": mfi_ddb.Streamer.SCHEMA.model_json_schema(),
    }


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


def _next_connection_id() -> str:
    """
    Generate the next connection id from what the backend already knows
    about (active_connections includes stopped-but-remembered connections,
    not just currently streaming ones) - the client should never have to
    guess a non-colliding id itself.
    """
    numeric_ids = [int(cid) for cid in active_connections.keys() if cid.isdigit()]
    return str(max(numeric_ids, default=100) + 1)


@router.post("/connect")
async def connection_create(
    adapter_name: str = Form(...),
    adapter_file: UploadFile = File(None),
    adapter_text: str = Form(None),
    streamer_file: UploadFile = File(None),
    streamer_text: str = Form(None),
    is_polling: bool = Form(True),
    polling_rate_hz: int = Form(1),
) -> dict:
    """
    Create a brand new connection. The backend generates its id - use
    POST /connect/{conn_id} instead to reconnect an existing one.

    Returns:
        Connection result including the generated id, streaming mode, and status

    Raises:
        HTTPException: 502 if connection or streaming setup fails
    """
    conn_id = _next_connection_id()

    adapter_cfg = utils.load_config(adapter_file, adapter_text)
    streamer_cfg = utils.load_config(streamer_file, streamer_text)

    connection = AdapterFactory(
        adp_name=adapter_name,
        adp_cfg=adapter_cfg,
        streamer_cfg=streamer_cfg,
        is_polling=is_polling,
        polling_rate_hz=polling_rate_hz,
    )

    try:
        await run_in_threadpool(connection.connect_and_stream)
    except Exception as err:
        logger.exception("connect_and_stream failed for new connection (adapter=%s)", adapter_name)
        raise HTTPException(status_code=502, detail=f"Connection failed: {err}")

    active_connections[conn_id] = connection
    connection_store.save_connection(
        conn_id, connection.adp_name, connection.adp_cfg, connection.streamer_cfg,
        connection.is_polling, connection.polling_rate_hz, desired_state="streaming", db_path=DB_PATH,
    )
    logger.info("Created connection: conn_id=%s", conn_id)

    return {
        "id": conn_id,
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }


@router.post("/connect/{conn_id}")
async def connection_connect(
    conn_id: str = FastAPIPath(...),
    adapter_name: str = Form(...),
) -> dict:
    """
    Reconnect an existing connection (e.g. after it was Stopped).

    Does not create new connections - use POST /connect (no id) for that,
    which lets the backend generate a non-colliding id. Always reconnects
    using the connection's already-stored config; to change config, use
    POST /update/{conn_id} instead.

    Raises:
        HTTPException: 404 if the connection doesn't exist, 502 if
        connection or streaming setup fails
    """
    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]
    if connection.adp_name != adapter_name:
        raise HTTPException(
            status_code=400,
            detail=f"Connection ID '{conn_id}' already exists with a different adapter '{connection.adp_name}'.",
        )

    if not connection.is_connected:
        try:
            await run_in_threadpool(connection.connect_and_stream)
        except Exception as err:
            logger.exception("connect_and_stream failed for conn_id=%s", conn_id)
            raise HTTPException(status_code=502, detail=f"Connection failed: {err}")
        connection_store.update_desired_state(conn_id, "streaming", db_path=DB_PATH)

    elif not connection.is_streaming:
        try:
            await run_in_threadpool(connection.resume_streaming)
        except Exception as err:
            raise HTTPException(
                status_code=502, detail=f"Streaming resume failed: {err}"
            )
        connection_store.update_desired_state(conn_id, "streaming", db_path=DB_PATH)

    return {
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }


@router.post("/update/{conn_id}")
async def connection_update(
    conn_id: str = FastAPIPath(...),
    adapter_file: UploadFile = File(None),
    adapter_text: str = Form(None),
    streamer_file: UploadFile = File(None),
    streamer_text: str = Form(None),
    is_polling: bool = Form(True),
    polling_rate_hz: int = Form(1),
) -> dict:
    """
    Apply an updated configuration to an existing connection.

    The adapter type is fixed at creation and cannot change here - only the
    adapter/streamer config, polling mode, and polling rate can be edited.

    Two paths:
    - Cadence-only change (still polling, only polling_rate_hz differs): the
      running poll loop reads polling_rate_hz fresh every iteration, so it's
      mutated in place with zero interruption. Nothing about the data source
      or Sparkplug metric set changed, so there's no birth/death implication.
    - Any other change: a new AdapterFactory is built and connected with the
      new config BEFORE the old one is touched. Only once the new one
      successfully connects (publishing a fresh Sparkplug BIRTH) is the old
      one disconnected (publishing DEATH) and swapped out. If the new config
      fails to connect, the old connection is left completely untouched and
      keeps running - an edit can fail without losing the existing connection.
    """
    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    old_connection = active_connections[conn_id]

    adapter_cfg = utils.load_config(adapter_file, adapter_text)
    streamer_cfg = utils.load_config(streamer_file, streamer_text)

    is_cadence_only_change = (
        old_connection.is_polling
        and is_polling
        and adapter_cfg == old_connection.adp_cfg
        and streamer_cfg == old_connection.streamer_cfg
    )
    if is_cadence_only_change:
        old_connection.polling_rate_hz = polling_rate_hz
        connection_store.update_polling_rate(conn_id, polling_rate_hz, db_path=DB_PATH)
        return {
            "updated": True,
            "reconnected": False,
            "is_connected": old_connection.is_connected,
            "is_streaming": old_connection.is_streaming,
        }

    new_connection = AdapterFactory(
        adp_name=old_connection.adp_name,
        adp_cfg=adapter_cfg,
        streamer_cfg=streamer_cfg,
        is_polling=is_polling,
        polling_rate_hz=polling_rate_hz,
    )
    try:
        await run_in_threadpool(new_connection.connect_and_stream)
    except Exception as err:
        logger.exception("Failed to apply updated configuration for conn_id=%s", conn_id)
        raise HTTPException(status_code=502, detail=f"Failed to apply updated configuration: {err}")

    # New connection is live; retire the old one. Best-effort: if the old
    # connection's disconnect fails, still swap in the new one rather than
    # leaving a working connection un-adopted.
    try:
        await run_in_threadpool(old_connection.disconnect)
    except Exception as err:
        logger.warning("Old connection disconnect failed during update for conn_id=%s: %s", conn_id, err)

    active_connections[conn_id] = new_connection
    connection_store.save_connection(
        conn_id, new_connection.adp_name, new_connection.adp_cfg, new_connection.streamer_cfg,
        new_connection.is_polling, new_connection.polling_rate_hz, desired_state="streaming", db_path=DB_PATH,
    )

    return {
        "updated": True,
        "reconnected": True,
        "is_connected": new_connection.is_connected,
        "is_streaming": new_connection.is_streaming,
    }


@router.post("/resume/{conn_id}")
async def connection_resume(
    conn_id: str = FastAPIPath(...),
) -> dict:

    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")
    
    connection = active_connections[conn_id]
    try:
        await run_in_threadpool(connection.resume_streaming)
    except Exception as err:
        raise HTTPException(
            status_code=502, detail=f"Streaming resume failed: {err}"
        )
    connection_store.update_desired_state(conn_id, "streaming", db_path=DB_PATH)

    return {
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }


@router.post("/pause/{conn_id}")
async def connection_pause(
    conn_id: str = FastAPIPath(...)
) -> dict:
    """Pause streaming but keep adapter instance alive."""
    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]
    try:
        await run_in_threadpool(connection.pause_streaming)
    except Exception as err:
        raise HTTPException(status_code=502, detail=f"Streaming pause failed: {err}")
    connection_store.update_desired_state(conn_id, "paused", db_path=DB_PATH)

    return {
        "is_connected": connection.is_connected,
        "is_streaming": connection.is_streaming,
        "mode": "polling" if connection.is_polling else "callback",
    }


@router.post("/stop/{conn_id}")
async def connection_stop(
    conn_id: str = FastAPIPath(...)
) -> dict:
    """
    Stop a connection but keep it known to the backend.

    The AdapterFactory instance (and its adp_cfg/streamer_cfg) is retained in
    active_connections so /all still lists it and /connect/{conn_id} can
    reconnect it later without needing the config to be remembered client-side.
    Use POST /delete/{conn_id} to fully forget a connection instead.
    """

    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]
    try:
        await run_in_threadpool(connection.disconnect)
    except Exception as err:
        raise HTTPException(status_code=502, detail=f"Stop failed: {err}")

    if connection.is_connected:
        raise HTTPException(status_code=502, detail="Stop failed: still connected")

    connection_store.update_desired_state(conn_id, "stopped", db_path=DB_PATH)
    return {"stopped": True}


@router.post("/delete/{conn_id}")
async def connection_delete(
    conn_id: str = FastAPIPath(...)
) -> dict:
    """
    Stop (if needed) and fully forget a connection.

    Deletion always proceeds even if the underlying stop (disconnect) call
    fails (e.g. broker unreachable) - a connection the user explicitly asked
    to remove should not get stuck in active_connections forever just because
    its teardown couldn't reach the network. The failure is logged instead.
    """

    if conn_id not in active_connections:
        raise HTTPException(status_code=404, detail="Connection not found")

    connection = active_connections[conn_id]
    if connection.is_connected:
        try:
            await run_in_threadpool(connection.disconnect)
        except Exception as err:
            logger.warning("stop (disconnect) failed while deleting conn_id=%s, deleting anyway: %s", conn_id, err)

    del active_connections[conn_id]
    connection_store.delete_connection(conn_id, db_path=DB_PATH)
    return {"deleted": True}