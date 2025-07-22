"""
config.py

FastAPI router for managing data adapter configurations in the DDB project.
Provides endpoints to validate, test, connect, stream, publish, and disconnect adapters
across supported protocols: MTConnect, ROS, Local Files, and MQTT.

Endpoints:
  POST   /validate                   : Validate config schema without network calls.
  POST   /connect/{conn_id}          : Instantiate adapter and start streaming.
  GET    /streaming-status/{conn_id} : Retrieve adapter connection and streaming status.
  GET    /stream/{conn_id}           : Server-Sent Events endpoint for real-time streaming.
  POST   /publish                    : Trigger a one-off MQTT publish event.
  POST   /disconnect/{conn_id}       : Stop streaming and disconnect adapter.
"""

import pkgutil
import importlib
import inspect
import socket
import time
import asyncio
from datetime import datetime  # Fix: Import datetime properly
from fastapi import APIRouter, Path, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse

from mfi_ddb.streamer import Streamer
from api.data_adapters.utils.loader import load_config
from api.data_adapters.utils.stream import event_stream, publish_once
from api.data_adapters.utils.validator import run_validation

# ──── Adapter Factory ──────────────────────
class AdapterFactory:
    """
    Factory to auto-discover and instantiate data adapters with optional async connect.

    Attributes:
        adapter_map (dict): Mapping from protocol name to DataAdapter class.
    """
    def __init__(self):
        # Discover available adapters on initialization
        self.adapter_map = self._discover_adapters()

    def _discover_adapters(self):
        """
        Auto-discover all DataAdapter subclasses in mfi_ddb.data_adapters package.

        Returns:
            dict: protocol_name -> DataAdapter class
        """
        import mfi_ddb.data_adapters as adapters_pkg
        amap = {}
        # Iterate modules in data_adapters package
        for _, module_name, _ in pkgutil.iter_modules(adapters_pkg.__path__):
            if module_name.startswith("_"):
                continue
            try:
                module = importlib.import_module(f"{adapters_pkg.__name__}.{module_name}")
                # Register classes ending with 'DataAdapter'
                for cls_name, cls_obj in inspect.getmembers(module, inspect.isclass):
                    if (cls_obj.__module__ == module.__name__ and cls_name.endswith("DataAdapter")):
                        amap[module_name] = cls_obj
            except ImportError as e:
                print(f"Warning: Could not import adapter module {module_name}: {e}")
        # Alias local_files as 'file'
        if 'local_files' in amap:
            amap['file'] = amap['local_files']
        return amap

    async def create_adapter(self, protocol: str, config: dict, auto_connect: bool = False, timeout: int = 30):
        """
        Instantiate and optionally connect a DataAdapter asynchronously.

        Args:
            protocol (str): Key to select adapter class from adapter_map.
            config (dict): Parsed configuration dictionary.
            auto_connect (bool): If True, call the adapter's connect() method.
            timeout (int): Max seconds to wait for creation and optional connect.

        Returns:
            object: Instantiated (and connected) adapter instance.

        Raises:
            ValueError: If protocol not supported.
            TimeoutError: If creation or connection exceeds timeout.
            RuntimeError: For other instantiation or connection errors.
        """
        if protocol not in self.adapter_map:
            raise ValueError(f"Unknown protocol: {protocol}")
        AdapterClass = self.adapter_map[protocol]

        loop = asyncio.get_event_loop()
         # Extract the protocol-specific config from the nested structure
        if protocol == 'mtconnect':
            # MTConnectDataAdapter wants the wrapper
            final_config = config
        elif protocol in config and isinstance(config[protocol], dict):
            # flatten for other adapters
            final_config = config[protocol]
        else:
            final_config = config
        try:
            # Create adapter in executor to avoid blocking event loop
            adapter = await asyncio.wait_for(
                loop.run_in_executor(None, lambda: AdapterClass(final_config)),
                timeout=timeout
            )
            # Connect if requested and method exists
            if auto_connect and hasattr(adapter, 'connect'):
                await asyncio.wait_for(
                    loop.run_in_executor(None, adapter.connect),
                    timeout=timeout
                )
            return adapter
        except asyncio.TimeoutError:
            raise TimeoutError(f"Adapter creation/connection timed out after {timeout} seconds")
        except Exception as e:
            raise RuntimeError(f"Failed to create {protocol} adapter: {e}")

# Initialize router and in-memory state stores
router = APIRouter()
factory = AdapterFactory()
ADAPTER_MAP = factory.adapter_map
# Transient storage for active connections and tasks
tmp_CONFIGS = {}         # conn_id -> (cfg_model, raw_data, protocol)
tmp_ADAPTERS = {}        # conn_id -> adapter instance
tmp_STREAMERS = {}       # conn_id -> Streamer or True for SSE
tmp_RATES = {}           # conn_id -> stream rate (Hz)
tmp_STOP_FLAGS = {}      # conn_id -> bool flag to stop streaming
tmp_BACKGROUND_TASKS = {}# conn_id -> asyncio.Task
tmp_CONNECTION_ERRORS = {}   # conn_id -> connection error messages
tmp_LAST_SUCCESSFUL_POLL = {} # conn_id -> timestamp of last successful poll

def check_mqtt_broker_connectivity(broker_host, broker_port, timeout=3):
    """Quick TCP connectivity check for MQTT broker"""
    try:
        print(f"DEBUG: Checking broker connectivity to {broker_host}:{broker_port}")
        with socket.create_connection((broker_host, broker_port), timeout=timeout) as sock:
            print(f"DEBUG: Broker {broker_host}:{broker_port} is reachable")
            return True, None
    except (socket.timeout, ConnectionRefusedError, socket.gaierror) as e:
        print(f"DEBUG: Broker {broker_host}:{broker_port} failed: {e}")
        return False, str(e)
    except Exception as e:
        print(f"DEBUG: Broker {broker_host}:{broker_port} error: {e}")
        return False, f"Network error: {str(e)}"
    
def pick_protocol(cfg) -> str:
    """
    Determine protocol key from loaded config model attributes.

    Args:
        cfg: Pydantic model with fields for various protocols.
    Returns:
        str: protocol identifier.
    Raises:
        HTTPException: 400 if no valid protocol detected.
    """
    if getattr(cfg, 'mtconnect', None):
        return 'mtconnect'
    if getattr(cfg, 'ros', None):
        return 'ros'
    if getattr(cfg, 'ros_files', None):
        return 'ros_files'
    if getattr(cfg, 'file', None):  # REMOVE the fallback check
        return 'file'
    if getattr(cfg, 'mqtt_adp', None):  # Add MQTT-ADP support
        return 'mqtt_adp'
    raise HTTPException(status_code=400, detail='No valid source protocol')

def _clean_data(data: dict) -> dict:
    """
    Decode any byte values to UTF-8 strings for adapter initialization.
    """
    return {k: (v.decode('utf-8', errors='ignore') if isinstance(v, bytes) else v)
            for k, v in data.items()}


@router.post(
    '/validate',
    summary="Validate adapter YAML",
    description="""
    Validate your adapter configuration against the schema.
    
    **Tips for using the text area:**
    - The text area might appear small, but you can resize it by dragging the bottom-right corner
    - Or create a .yaml file and upload it instead
    - Make sure to preserve line breaks in your YAML
    
    **Example Configuration:**
    ```yaml
    agent_ip: agent.mtconnect.org
    agent_url: 'http://agent.mtconnect.org/'
    stream_rate: 5
    device_name: 'GFAgie01'
    trial_id: 'nist_test'
    ```
    """
)
async def validate(
    file: UploadFile = File(None),
    text: str = Form(None)
) -> dict:
    """
    Schema-validate adapter configuration without any network I/O.

    Accepts either a file upload or raw YAML text.

    Returns:
        {'valid': True} on success, HTTP 400 on schema error.
    """
    cfg = await load_config(file, text)
    proto = pick_protocol(cfg)
    data = cfg.model_dump()
    try:
        run_validation(data, proto)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=f'Schema error: {e}')
    return {'valid': True}


@router.post('/connect/{conn_id}')
async def connect(
    conn_id: str,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(None),
    text: str = Form(None)
) -> dict:
    """
    Instantiate a data adapter and begin continuous or SSE streaming.

    If an existing config id is provided, reuse stored settings; otherwise load anew.
    """
    tmp_STOP_FLAGS[conn_id] = False
    if conn_id in tmp_CONFIGS:
        cfg, data, proto = tmp_CONFIGS[conn_id]
    else:
        cfg = await load_config(file, text)
        proto = pick_protocol(cfg)
        data = cfg.model_dump()
        tmp_CONFIGS[conn_id] = (cfg, data, proto)
    # Create and optionally connect adapter
    try:
        adapter = await factory.create_adapter(proto, _clean_data(data), auto_connect=True)
    except TimeoutError as e:
        raise HTTPException(status_code=502, detail=f'Connection timed out: {e}')
    except Exception as e:
        raise HTTPException(status_code=502, detail=f'Connection failed: {e}')
    tmp_ADAPTERS[conn_id] = adapter
    rate = int(getattr(getattr(cfg, proto, None), 'stream_rate', 1))
    tmp_RATES[conn_id] = rate

    # Setup MQTT streaming via Streamer
    # Setup MQTT streaming via Streamer
    if getattr(cfg, 'mqtt', None):
        try:
            # Add timeout wrapper for Streamer creation to prevent infinite blocking
            streamer_creation_timeout = 30  # 30 seconds max
            
            async def create_streamer_with_timeout():
                loop = asyncio.get_running_loop()
                return await asyncio.wait_for(
                    loop.run_in_executor(None, lambda: Streamer(
                        {'topic_family': cfg.topic_family, 'mqtt': cfg.mqtt.model_dump()},
                        adapter,
                        stream_on_update=False
                    )),
                    timeout=streamer_creation_timeout
                )
            
            streamer = await create_streamer_with_timeout()
            
        except asyncio.TimeoutError:
            # Cleanup on timeout
            tmp_ADAPTERS.pop(conn_id, None)
            tmp_RATES.pop(conn_id, None)
            tmp_CONFIGS.pop(conn_id, None)
            tmp_STREAMERS.pop(conn_id, None)
            tmp_STOP_FLAGS.pop(conn_id, None)
            
            raise HTTPException(
                status_code=502, 
                detail='MQTT broker unreachable: Connection timed out after 30 seconds'
            )
        except Exception as e:
            # Cleanup on other errors
            tmp_ADAPTERS.pop(conn_id, None)
            tmp_RATES.pop(conn_id, None)
            tmp_CONFIGS.pop(conn_id, None)
            tmp_STREAMERS.pop(conn_id, None)
            tmp_STOP_FLAGS.pop(conn_id, None)
            task = tmp_BACKGROUND_TASKS.pop(conn_id, None)
            if task:
                task.cancel()
            
            error_msg = str(e).lower()
            if "getaddrinfo failed" in error_msg or "11001" in error_msg:
                raise HTTPException(
                    status_code=502, 
                    detail='MQTT broker unreachable: Cannot resolve broker hostname'
                )
            elif "no connection could be made" in error_msg or "refused" in error_msg:
                raise HTTPException(status_code=502, detail='MQTT broker unreachable')
            else:
                raise HTTPException(status_code=502, detail=f'MQTT connection failed: {e}')
                
        tmp_STREAMERS[conn_id] = streamer

        # Fix: Move the function definition outside and fix indentation
        async def continuous_polling():
            """Background task to poll adapter and publish to MQTT."""
            loop = asyncio.get_running_loop()
            consecutive_failures = 0
            
            while not tmp_STOP_FLAGS.get(conn_id, False):
                try:
                    # Just poll and stream data - no broker checks for now
                    await loop.run_in_executor(None, streamer.poll_and_stream_data, rate)
                    
                    # Success - reset counters
                    consecutive_failures = 0
                    tmp_LAST_SUCCESSFUL_POLL[conn_id] = datetime.now().isoformat()
                    if conn_id in tmp_CONNECTION_ERRORS:
                        print(f"DEBUG: Clearing connection errors for {conn_id}")
                        del tmp_CONNECTION_ERRORS[conn_id]
                        
                except Exception as e:
                    print(f"DEBUG: Streaming exception for {conn_id}: {e}")
                    
                    # Categorize the error properly
                    error_msg = str(e).lower()
                    if any(word in error_msg for word in ['connection refused', 'host unreachable', 'timeout', 'no route']):
                        tmp_CONNECTION_ERRORS[conn_id] = f"Broker unreachable: {str(e)}"
                    elif any(word in error_msg for word in ['authentication', 'credential', 'unauthorized']):
                        tmp_CONNECTION_ERRORS[conn_id] = f"Broker auth failed: {str(e)}"
                    elif any(word in error_msg for word in ['disconnected', 'connection lost', 'not connected']):
                        tmp_CONNECTION_ERRORS[conn_id] = f"Broker disconnected: {str(e)}"
                    else:
                        tmp_CONNECTION_ERRORS[conn_id] = f"Streaming error: {str(e)}"
                    
                    consecutive_failures += 1
                    if consecutive_failures >= 3:
                        print(f"DEBUG: Max failures reached for {conn_id}, stopping streaming")
                        break
                    
                    # Wait before retry
                    await asyncio.sleep(5)
                    
                await asyncio.sleep(1 / rate)
            
            print(f"DEBUG: Streaming task ended for {conn_id}")

        # Fix: Create task OUTSIDE the function
        task = asyncio.create_task(continuous_polling())
        tmp_BACKGROUND_TASKS[conn_id] = task
        return {'connected': True, 'streaming_to_broker': True}

    # Fallback to SSE-only streaming
    tmp_STREAMERS[conn_id] = True
    return {'connected': True, 'streaming_via_sse': True}


@router.get('/streaming-status/{conn_id}')
async def streaming_status(
    conn_id: str = Path(...)
) -> dict:
    """
    Retrieve current connection and streaming status for given id.
    """
    is_streaming = conn_id in tmp_STREAMERS
    
    # Check if background task is healthy
    broker_connected = False
    task_error = None
    
    if conn_id in tmp_BACKGROUND_TASKS:
        task = tmp_BACKGROUND_TASKS[conn_id]
        broker_connected = not task.done() and not task.cancelled()
        
        if task.done() and not task.cancelled():
            try:
                task.result()
            except Exception as e:
                task_error = str(e)
    
    # Check for stored connection errors
    connection_error = tmp_CONNECTION_ERRORS.get(conn_id) or task_error
    
    return {
        'adapter_connected': conn_id in tmp_ADAPTERS,
        'is_streaming': is_streaming,
        'broker_connected': broker_connected,
        'connection_error': connection_error,
        'stream_rate': tmp_RATES.get(conn_id),
        'last_successful_poll': tmp_LAST_SUCCESSFUL_POLL.get(conn_id)
    }


@router.get('/stream/{conn_id}')
async def stream(
    conn_id: str = Path(...)
) -> StreamingResponse:
    """
    Server-Sent Events endpoint sending real-time adapter data.

    Requires prior /connect call for this conn_id.
    """
    adapter = tmp_ADAPTERS.get(conn_id)
    rate = tmp_RATES.get(conn_id)
    if not adapter or rate is None:
        raise HTTPException(status_code=400, detail='Must call /connect first')
    return StreamingResponse(event_stream(adapter, rate), media_type='text/event-stream')


@router.post('/publish')
async def publish(
    background_tasks: BackgroundTasks,
    id: str = Form(...)
) -> dict:
    """
    Trigger a one-time MQTT publish using stored adapter config.
    """
    entry = tmp_CONFIGS.get(id)
    if not entry:
        raise HTTPException(status_code=400, detail='No such connection ID')
    cfg, _, _ = entry
    if not getattr(cfg, 'mqtt', None):
        raise HTTPException(status_code=400, detail='Cannot publish without MQTT block')
    adapter_cfg = {'mqtt': cfg.mqtt.model_dump(), 'topic_family': cfg.topic_family}
    background_tasks.add_task(publish_once, adapter_cfg, cfg.topic_family)
    return {'published': True}


@router.post('/disconnect/{conn_id}')
async def disconnect(conn_id: str = Path(...)) -> dict:
    """
    Stop streaming and disconnect adapter - generic for all types.
    """
    print(f"DEBUG: Disconnecting {conn_id}")
    
    # Step 1: Stop background task
    tmp_STOP_FLAGS[conn_id] = True
    task = tmp_BACKGROUND_TASKS.pop(conn_id, None)
    if task:
        task.cancel()
        try:
            await asyncio.wait_for(task, timeout=3.0)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            pass
    
    # Step 2: Let streamer handle its own cleanup
    streamer = tmp_STREAMERS.pop(conn_id, None)
    if streamer and hasattr(streamer, 'disconnect'):
        try:
            # This will now properly disconnect both Mqtt and MqttSpb clients
            streamer.disconnect()  # Calls MqttSpb.disconnect() which now works!
        except Exception as e:
            print(f"DEBUG: Error during disconnect: {e}")
    
    # Step 3: Disconnect adapter
    adapter = tmp_ADAPTERS.pop(conn_id, None)
    if adapter and hasattr(adapter, 'disconnect'):
        try:
            adapter.disconnect()
        except Exception as e:
            print(f"DEBUG: Error disconnecting adapter: {e}")
    
    # Step 4: Clean up state
    tmp_RATES.pop(conn_id, None)
    tmp_CONFIGS.pop(conn_id, None)
    tmp_CONNECTION_ERRORS.pop(conn_id, None)
    tmp_LAST_SUCCESSFUL_POLL.pop(conn_id, None)
    
    print(f"DEBUG: Disconnect completed for {conn_id}")
    return {'disconnected': True}