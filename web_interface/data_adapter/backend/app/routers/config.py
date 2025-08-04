"""
config.py - localStorage-Only Adapter Management with Pause/Resume

Endpoints:
 POST   /validate                   : Validate config schema
 POST   /connect/{conn_id}          : Connect adapter and start MQTT streaming
 POST   /pause/{conn_id}           : Pause streaming but keep adapter connected
 POST   /resume/{conn_id}          : Resume paused streaming
 GET    /streaming-status/{conn_id} : Get detailed status (active/inactive/paused + reasons)
 GET    /stream/{conn_id}          : SSE status stream
 POST   /publish                   : One-time MQTT publish
 POST   /disconnect/{conn_id}      : Disconnect adapter
 GET    /health                    : Health check endpoint
"""

import pkgutil
import importlib
import inspect
import asyncio
import datetime

from fastapi import Path as FastAPIPath
from fastapi.responses import StreamingResponse
from mfi_ddb.streamer import Streamer
from contextlib import asynccontextmanager

from app.utils.loader import load_config
from app.utils.validator import run_validation
from app.utils.stream import publish_once 
from fastapi import APIRouter, UploadFile, File, Form, HTTPException, BackgroundTasks

# Enhanced Adapter Factory
class AdapterFactory:
   def __init__(self):
       self.adapter_map = self._discover_adapters()

   def _discover_adapters(self):
       """Discover all available adapter modules"""
       import mfi_ddb.data_adapters as adapters_pkg
       amap = {}
       
       for _, module_name, _ in pkgutil.iter_modules(adapters_pkg.__path__):
           if module_name.startswith("_"):
               continue
           try:
               module = importlib.import_module(f"{adapters_pkg.__name__}.{module_name}")
               for cls_name, cls_obj in inspect.getmembers(module, inspect.isclass):
                   if (cls_obj.__module__ == module.__name__ and cls_name.endswith("DataAdapter")):
                       amap[module_name] = cls_obj
           except ImportError as e:
               print(f"Warning: Could not import adapter module {module_name}: {e}")
       
       if 'local_files' in amap:
           amap['file'] = amap['local_files']
           
       return amap

   async def create_adapter(self, protocol: str, config: dict, auto_connect: bool = False, timeout: int = 30):
       """Create adapter instance for any protocol"""
       if protocol not in self.adapter_map:
           raise ValueError(f"Unknown protocol: {protocol}")
       AdapterClass = self.adapter_map[protocol]

       loop = asyncio.get_event_loop()
       if protocol == 'mtconnect':
           final_config = config
       elif protocol in config and isinstance(config[protocol], dict):
           final_config = config[protocol]
       else:
           final_config = config
       try:
           adapter = await asyncio.wait_for(
               loop.run_in_executor(None, lambda: AdapterClass(final_config)),
               timeout=timeout
           )
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

   def supports_callbacks(self, protocol: str, adapter=None) -> bool:
       """Check if adapter supports callback-based streaming"""
       callback_protocols = {
           'mqtt_adp': True,
           'file': True,
           'local_files': True,
           'ros': 'dynamic',
           'ros_files': 'dynamic'
       }
       
       if protocol not in callback_protocols:
           return False
       
       capability = callback_protocols[protocol]
       if capability == 'dynamic':
           if adapter and hasattr(adapter, 'cfg'):
               return adapter.cfg.get('set_ros_callback', False)
           return False
       
       return capability

   def disconnect_adapter(self, protocol: str, adapter) -> bool:
       """Generic adapter disconnect"""
       try:
           if hasattr(adapter, 'disconnect'):
               adapter.disconnect()
               return True
           return True
       except Exception as e:
           print(f"Error disconnecting {protocol} adapter: {e}")
           return False

# Initialize router and storage
router = APIRouter()
factory = AdapterFactory()
ADAPTER_MAP = factory.adapter_map

# In-memory storage for active connections (localStorage handles persistence)
tmp_CONFIGS = {}
tmp_ADAPTERS = {}
tmp_STREAMERS = {}
tmp_RATES = {}
tmp_STOP_FLAGS = {}
tmp_BACKGROUND_TASKS = {}
tmp_CONNECTION_ERRORS = {}
tmp_LAST_SUCCESSFUL_POLL = {}
tmp_PAUSE_STATES = {}  # Track explicit pause states

# Helper functions
def pick_protocol(cfg) -> str:
   """Detect protocol from configuration"""
   if getattr(cfg, 'mtconnect', None):
       return 'mtconnect'
   if getattr(cfg, 'ros', None):
       return 'ros'
   if getattr(cfg, 'ros_files', None):
       return 'ros_files'
   if getattr(cfg, 'file', None):
       return 'file'
   if getattr(cfg, 'mqtt_adp', None):
       return 'mqtt_adp'
   raise HTTPException(status_code=400, detail='No valid source protocol')

def _clean_data(data: dict) -> dict:
   """Clean byte strings from configuration data"""
   return {k: (v.decode('utf-8', errors='ignore') if isinstance(v, bytes) else v)
           for k, v in data.items()}

def _cleanup_connection(conn_id: str):
   """Clean up connection state from memory"""
   tmp_RATES.pop(conn_id, None)
   tmp_CONFIGS.pop(conn_id, None)
   tmp_CONNECTION_ERRORS.pop(conn_id, None)
   tmp_LAST_SUCCESSFUL_POLL.pop(conn_id, None)
   tmp_STOP_FLAGS.pop(conn_id, None)
   tmp_PAUSE_STATES.pop(conn_id, None)

async def _continuous_polling(conn_id: str, streamer, rate: int):
   """Background polling for adapters that don't support callbacks"""
   consecutive_failures = 0
   
   # Ensure rate is valid
   try:
       rate = float(rate) if rate else 1.0
       sleep_interval = 1.0 / rate
   except (ValueError, ZeroDivisionError):
       rate = 1.0
       sleep_interval = 1.0
   
   print(f"DEBUG: Starting polling for {conn_id} at rate {rate} Hz (sleep: {sleep_interval}s)")
   
   while not tmp_STOP_FLAGS.get(conn_id, False):
       try:
           await asyncio.get_running_loop().run_in_executor(
               None, streamer.poll_and_stream_data, int(rate)
           )
           
           consecutive_failures = 0
           tmp_LAST_SUCCESSFUL_POLL[conn_id] = datetime.datetime.now().isoformat()
           if conn_id in tmp_CONNECTION_ERRORS:
               print(f"DEBUG: Clearing connection error after successful poll for {conn_id}")
               del tmp_CONNECTION_ERRORS[conn_id]
               
       except Exception as e:
           print(f"Polling exception for {conn_id}: {e}")
           
           # Categorize errors for better status reporting
           error_msg = str(e).lower()
           if any(word in error_msg for word in ['connection refused', 'host unreachable', 'timeout', 'no route']):
               tmp_CONNECTION_ERRORS[conn_id] = f"Broker unreachable: {str(e)}"
           elif any(word in error_msg for word in ['authentication', 'credential', 'unauthorized']):
               tmp_CONNECTION_ERRORS[conn_id] = f"Broker auth failed: {str(e)}"
           elif any(word in error_msg for word in ['disconnected', 'connection lost', 'not connected']):
               tmp_CONNECTION_ERRORS[conn_id] = f"Broker disconnected: {str(e)}"
           elif 'float' in error_msg and 'integer' in error_msg:
               tmp_CONNECTION_ERRORS[conn_id] = f"Configuration error: Invalid stream rate"
           else:
               tmp_CONNECTION_ERRORS[conn_id] = f"Streaming error: {str(e)}"
           
           consecutive_failures += 1
           if consecutive_failures >= 3:
               tmp_CONNECTION_ERRORS[conn_id] = f"Streaming stopped after {consecutive_failures} failures: {str(e)}"
               print(f"Max failures reached for {conn_id}, stopping")
               break
           
           await asyncio.sleep(5)  # Wait before retry
           
       await asyncio.sleep(sleep_interval)

@router.get('/health')
async def health_check():
   """Health check endpoint"""
   return {
       'status': 'healthy',
       'timestamp': datetime.datetime.now().isoformat(),
       'active_connections': len(tmp_ADAPTERS),
       'streaming_connections': len(tmp_STREAMERS)
   }

@router.post('/validate')
async def validate(
   file: UploadFile = File(None),
   text: str = Form(None)
) -> dict:
   """Validate configuration schema"""
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
   """Connect adapter and start MQTT streaming"""
   print(f"DEBUG: Connecting {conn_id}")
   
   tmp_STOP_FLAGS[conn_id] = False
   tmp_PAUSE_STATES.pop(conn_id, None)  # Clear explicit pause state
   
   if conn_id in tmp_CONFIGS:
       cfg, data, proto = tmp_CONFIGS[conn_id]
   else:
       cfg = await load_config(file, text)
       proto = pick_protocol(cfg)
       data = cfg.model_dump()
       tmp_CONFIGS[conn_id] = (cfg, data, proto)

   try:
       adapter = await factory.create_adapter(proto, _clean_data(data), auto_connect=True)
   except TimeoutError as e:
       raise HTTPException(status_code=502, detail=f'Connection timed out: {e}')
   except Exception as e:
       raise HTTPException(status_code=502, detail=f'Connection failed: {e}')
   
   tmp_ADAPTERS[conn_id] = adapter
   rate = int(getattr(getattr(cfg, proto, None), 'stream_rate', 1))
   tmp_RATES[conn_id] = rate

   # NO FILE SAVING - frontend localStorage handles all persistence

   # Setup MQTT streaming
   if getattr(cfg, 'mqtt', None):
       if conn_id in tmp_CONNECTION_ERRORS:
           del tmp_CONNECTION_ERRORS[conn_id]
           
       try:
           supports_callbacks = factory.supports_callbacks(proto, adapter)
           
           streamer = await asyncio.wait_for(
               asyncio.get_running_loop().run_in_executor(
                   None, 
                   lambda: Streamer(
                       {'topic_family': cfg.topic_family, 'mqtt': cfg.mqtt.model_dump()},
                       adapter,
                       stream_on_update=supports_callbacks
                   )
               ),
               timeout=30
           )
           
           tmp_STREAMERS[conn_id] = streamer
           
           if conn_id in tmp_CONNECTION_ERRORS:
               del tmp_CONNECTION_ERRORS[conn_id]
           
           if not supports_callbacks:
               task = asyncio.create_task(_continuous_polling(conn_id, streamer, rate))
               tmp_BACKGROUND_TASKS[conn_id] = task
               
           print(f"DEBUG: Successfully connected {conn_id} in streaming mode")
           return {
               'connected': True, 
               'streaming_to_broker': True, 
               'mode': 'callback' if supports_callbacks else 'polling'
           }
               
       except asyncio.TimeoutError:
           _cleanup_connection(conn_id)
           tmp_CONNECTION_ERRORS[conn_id] = "MQTT broker unreachable: Connection timed out"
           raise HTTPException(status_code=502, detail='MQTT broker unreachable: Connection timed out')
       except Exception as e:
           _cleanup_connection(conn_id)
           error_msg = str(e).lower()
           if "getaddrinfo failed" in error_msg or "11001" in error_msg:
               tmp_CONNECTION_ERRORS[conn_id] = "MQTT broker unreachable: Cannot resolve hostname"
               raise HTTPException(status_code=502, detail='MQTT broker unreachable: Cannot resolve hostname')
           elif "no connection could be made" in error_msg or "refused" in error_msg:
               tmp_CONNECTION_ERRORS[conn_id] = "MQTT broker unreachable"
               raise HTTPException(status_code=502, detail='MQTT broker unreachable')
           else:
               tmp_CONNECTION_ERRORS[conn_id] = f"MQTT connection failed: {e}"
               raise HTTPException(status_code=502, detail=f'MQTT connection failed: {e}')
   else:
       print(f"DEBUG: Connected {conn_id} without MQTT")
       return {'connected': True, 'streaming_to_broker': False, 'note': 'No MQTT configured'}

@router.post('/pause/{conn_id}')
async def pause_connection(conn_id: str = FastAPIPath(...)) -> dict:
   """Pause a connection - stops streaming but keeps adapter connected"""
   print(f"DEBUG: Pausing connection {conn_id}")
   
   if conn_id not in tmp_ADAPTERS:
       raise HTTPException(status_code=404, detail='Connection not found')
   
   try:
       # Set explicit pause state
       tmp_PAUSE_STATES[conn_id] = True
       
       # Stop background task
       tmp_STOP_FLAGS[conn_id] = True
       task = tmp_BACKGROUND_TASKS.get(conn_id)
       if task and not task.done():
           task.cancel()
           try:
               await asyncio.wait_for(task, timeout=2.0)
           except (asyncio.CancelledError, asyncio.TimeoutError):
               pass
           tmp_BACKGROUND_TASKS.pop(conn_id, None)
       
       # Disconnect streamer but keep adapter
       streamer = tmp_STREAMERS.pop(conn_id, None)
       if streamer and hasattr(streamer, 'disconnect'):
           try:
               streamer.disconnect()
               print(f"DEBUG: Disconnected streamer for {conn_id}")
           except Exception as e:
               print(f"DEBUG: Error disconnecting streamer: {e}")
       
       # Clear connection errors
       tmp_CONNECTION_ERRORS.pop(conn_id, None)
       
       print(f"DEBUG: Successfully paused {conn_id}")
       return {'paused': True, 'connection_id': conn_id}
       
   except Exception as e:
       print(f"DEBUG: Error pausing {conn_id}: {e}")
       raise HTTPException(status_code=500, detail=f'Failed to pause connection: {e}')

@router.post('/resume/{conn_id}')
async def resume_connection(
    conn_id: str = FastAPIPath(...),
    file: UploadFile = File(None),
    text: str = Form(None)
) -> dict:
    """Resume a paused connection - always accepts config data since backend loses state on restart"""
    print(f"DEBUG: Resuming connection {conn_id}")
    
    # Always expect config data since backend loses state on restart
    if not file and not text:
        raise HTTPException(
            status_code=400, 
            detail='Configuration data required to resume connection. Please provide config data.'
        )
    
    # Load configuration from provided data
    try:
        cfg = await load_config(file, text)
        proto = pick_protocol(cfg)
        data = cfg.model_dump()
        
        # Store in memory
        tmp_CONFIGS[conn_id] = (cfg, data, proto)
        print(f"DEBUG: Loaded config for {conn_id}, protocol: {proto}")
        
    except Exception as e:
        raise HTTPException(status_code=400, detail=f'Invalid configuration: {e}')
    
    # If connection exists in memory from a previous operation, clean it up first
    if conn_id in tmp_ADAPTERS:
        print(f"DEBUG: Cleaning up existing adapter for {conn_id}")
        # Stop any existing background tasks
        tmp_STOP_FLAGS[conn_id] = True
        task = tmp_BACKGROUND_TASKS.pop(conn_id, None)
        if task and not task.done():
            task.cancel()
        
        # Disconnect existing streamer
        streamer = tmp_STREAMERS.pop(conn_id, None)
        if streamer and hasattr(streamer, 'disconnect'):
            try:
                streamer.disconnect()
            except Exception as e:
                print(f"DEBUG: Error disconnecting existing streamer: {e}")
    
    # Create and connect adapter
    try:
        adapter = await factory.create_adapter(proto, _clean_data(data), auto_connect=True)
        tmp_ADAPTERS[conn_id] = adapter
        rate = int(getattr(getattr(cfg, proto, None), 'stream_rate', 1))
        tmp_RATES[conn_id] = rate
        
        print(f"DEBUG: Created adapter for {conn_id}")
        
    except Exception as e:
        _cleanup_connection(conn_id)
        raise HTTPException(status_code=502, detail=f'Failed to create adapter: {e}')
    
    # Set up streaming in active/resumed state
    tmp_STOP_FLAGS[conn_id] = False
    tmp_PAUSE_STATES.pop(conn_id, None)  # Clear explicit pause state since we're resuming
    
    if getattr(cfg, 'mqtt', None):
        tmp_CONNECTION_ERRORS.pop(conn_id, None)
        
        try:
            supports_callbacks = factory.supports_callbacks(proto, adapter)
            
            streamer = await asyncio.wait_for(
                asyncio.get_running_loop().run_in_executor(
                    None, 
                    lambda: Streamer(
                        {'topic_family': cfg.topic_family, 'mqtt': cfg.mqtt.model_dump()},
                        adapter,
                        stream_on_update=supports_callbacks
                    )
                ),
                timeout=30
            )
            
            tmp_STREAMERS[conn_id] = streamer
            
            if not supports_callbacks:
                task = asyncio.create_task(_continuous_polling(conn_id, streamer, rate))
                tmp_BACKGROUND_TASKS[conn_id] = task
            
            print(f"DEBUG: Successfully resumed {conn_id} with streaming")
            return {
                'resumed': True,
                'connection_id': conn_id,
                'streaming_to_broker': True, 
                'mode': 'callback' if supports_callbacks else 'polling'
            }
                
        except Exception as e:
            _cleanup_connection(conn_id)
            error_msg = str(e).lower()
            if "getaddrinfo failed" in error_msg or "11001" in error_msg:
                tmp_CONNECTION_ERRORS[conn_id] = "MQTT broker unreachable: Cannot resolve hostname"
                raise HTTPException(status_code=502, detail='MQTT broker unreachable: Cannot resolve hostname')
            elif "no connection could be made" in error_msg or "refused" in error_msg:
                tmp_CONNECTION_ERRORS[conn_id] = "MQTT broker unreachable"
                raise HTTPException(status_code=502, detail='MQTT broker unreachable')
            else:
                tmp_CONNECTION_ERRORS[conn_id] = f"Resume failed: {e}"
                raise HTTPException(status_code=502, detail=f'Failed to resume connection: {e}')
    else:
        print(f"DEBUG: Resumed {conn_id} without MQTT")
        return {
            'resumed': True, 
            'connection_id': conn_id, 
            'streaming_to_broker': False, 
            'note': 'No MQTT configured'
        }
@router.get('/streaming-status/{conn_id}')
async def streaming_status(conn_id: str = FastAPIPath(...)) -> dict:
   """Get detailed connection status including pause state"""
   if conn_id not in tmp_CONFIGS:
       return {
           'status': 'not_found',
           'error': 'Connection not found - call /connect first',
           'adapter_connected': False,
           'is_streaming': False,
           'is_paused': False
       }
   
   _, _, proto = tmp_CONFIGS[conn_id]
   adapter = tmp_ADAPTERS.get(conn_id)
   streamer = tmp_STREAMERS.get(conn_id)
   
   # Determine overall status
   adapter_connected = conn_id in tmp_ADAPTERS
   is_streaming = conn_id in tmp_STREAMERS
   
   # Check if connection was explicitly paused
   is_explicitly_paused = tmp_PAUSE_STATES.get(conn_id, False)
   
   # For localStorage-only, pause state is primarily determined by explicit backend state
   is_paused = is_explicitly_paused or (adapter_connected and not is_streaming)
   
   # Check streaming mode
   streaming_mode = None
   if adapter:
       supports_callbacks = factory.supports_callbacks(proto, adapter)
       streaming_mode = 'callback' if supports_callbacks else 'polling'
   
   # Check recent polling success
   last_poll_time = tmp_LAST_SUCCESSFUL_POLL.get(conn_id)
   recent_success = False
   if last_poll_time:
       try:
           last_poll_dt = datetime.datetime.fromisoformat(last_poll_time)
           time_since_success = (datetime.datetime.now() - last_poll_dt).total_seconds()
           recent_success = time_since_success < 60
       except:
           recent_success = False
   
   # Check background task health
   background_task_status = 'not_applicable'
   task_healthy = True
   
   if conn_id in tmp_BACKGROUND_TASKS:
       task = tmp_BACKGROUND_TASKS[conn_id]
       if task.done():
           if task.cancelled():
               background_task_status = 'cancelled'
               task_healthy = False
           else:
               try:
                   task.result()
                   background_task_status = 'completed'
                   task_healthy = False
               except Exception as e:
                   background_task_status = f'failed: {str(e)}'
                   task_healthy = False
       else:
           background_task_status = 'running'
           task_healthy = True
   
   # Determine status with pause state
   if not adapter_connected:
       status = 'inactive'
       reason = 'Adapter not connected'
   elif is_paused:
       status = 'paused'
       reason = 'Streaming paused by user'
   elif not is_streaming:
       status = 'inactive'  
       reason = 'Streamer not initialized'
   elif streaming_mode == 'callback' and is_streaming:
       if conn_id in tmp_CONNECTION_ERRORS:
           status = 'inactive'
           reason = tmp_CONNECTION_ERRORS[conn_id]
       else:
           status = 'active'
           reason = 'Streaming via callbacks'
   elif streaming_mode == 'polling':
       if not task_healthy:
           status = 'inactive'
           reason = f'Background polling task {background_task_status}'
       elif conn_id in tmp_CONNECTION_ERRORS and not recent_success:
           status = 'inactive'
           reason = tmp_CONNECTION_ERRORS[conn_id]
       elif recent_success:
           status = 'active'
           reason = 'Streaming via polling'
       else:
           status = 'starting'
           reason = 'Polling task running, waiting for first success'
   else:
       status = 'unknown'
       reason = 'Unable to determine status'
   
   return {
       'status': status,
       'reason': reason,
       'adapter_connected': adapter_connected,
       'protocol': proto,
       'streaming_mode': streaming_mode,
       'is_streaming': is_streaming,
       'is_paused': is_paused,
       'is_explicitly_paused': is_explicitly_paused,
       'background_task_status': background_task_status,
       'task_healthy': task_healthy,
       'recent_success': recent_success,
       'stream_rate': tmp_RATES.get(conn_id),
       'last_successful_poll': tmp_LAST_SUCCESSFUL_POLL.get(conn_id),
       'connection_error': tmp_CONNECTION_ERRORS.get(conn_id),
       'timestamp': datetime.datetime.now().isoformat()
   }

@router.get('/stream/{conn_id}')
async def stream(conn_id: str = FastAPIPath(...)) -> StreamingResponse:
   """SSE endpoint for real-time status updates"""
   import json
   
   if conn_id not in tmp_ADAPTERS:
       raise HTTPException(status_code=404, detail='Connection not found')
   
   async def status_stream():
       counter = 0
       while True:
           try:
               status_data = await streaming_status(conn_id)
               
               payload = {
                   "status": status_data.get('status', 'unknown'),
                   "reason": status_data.get('reason', 'No reason'),
                   "streaming_mode": status_data.get('streaming_mode', 'unknown'),
                   "is_paused": status_data.get('is_paused', False),
                   "timestamp": datetime.datetime.now().isoformat(),
                   "counter": counter
               }
               
               yield f"data: {json.dumps(payload)}\n\n"
               counter += 1
               
           except Exception as e:
               error_payload = {
                   "error": str(e),
                   "timestamp": datetime.datetime.now().isoformat(),
                   "counter": counter
               }
               yield f"data: {json.dumps(error_payload)}\n\n"
               counter += 1
           
           await asyncio.sleep(1)
   
   return StreamingResponse(status_stream(), media_type='text/event-stream')

@router.post('/publish')
async def publish(
   background_tasks: BackgroundTasks,
   id: str = Form(...)
) -> dict:
   """One-time MQTT publish"""
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
async def disconnect(conn_id: str = FastAPIPath(...)) -> dict:
   """Disconnect adapter and clean up resources"""
   print(f"DEBUG: Disconnecting {conn_id}")
   
   # Stop background task
   tmp_STOP_FLAGS[conn_id] = True
   task = tmp_BACKGROUND_TASKS.pop(conn_id, None)
   if task:
       task.cancel()
       try:
           await asyncio.wait_for(task, timeout=3.0)
       except (asyncio.CancelledError, asyncio.TimeoutError):
           pass
   
   # Disconnect streamer
   streamer = tmp_STREAMERS.pop(conn_id, None)
   if streamer and hasattr(streamer, 'disconnect'):
       try:
           streamer.disconnect()
       except Exception as e:
           print(f"DEBUG: Error during disconnect: {e}")
   
   # Disconnect adapter
   adapter = tmp_ADAPTERS.pop(conn_id, None)
   if adapter and conn_id in tmp_CONFIGS:
       _, _, proto = tmp_CONFIGS[conn_id]
       factory.disconnect_adapter(proto, adapter)
   
   # NO FILE REMOVAL - frontend localStorage handles persistence
   
   # Clean up memory
   _cleanup_connection(conn_id)
   print(f"DEBUG: Disconnect completed for {conn_id}")
   return {'disconnected': True}

@asynccontextmanager
async def lifespan(app):
    """Lifespan event handler for FastAPI application"""
    # Startup
    print("Backend server starting up...")
    print("Using localStorage-only persistence - no file storage")
    
    yield
    
    # Shutdown
    print("Backend server shutting down...")
    
    # Cancel all background tasks
    for conn_id, task in tmp_BACKGROUND_TASKS.items():
        if not task.done():
            task.cancel()
            try:
                await asyncio.wait_for(task, timeout=2.0)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
    
    # Disconnect all streamers
    for conn_id, streamer in tmp_STREAMERS.items():
        if hasattr(streamer, 'disconnect'):
            try:
                streamer.disconnect()
            except Exception as e:
                print(f"Error disconnecting streamer {conn_id}: {e}")
    
    # Disconnect all adapters
    for conn_id, adapter in tmp_ADAPTERS.items():
        if conn_id in tmp_CONFIGS:
            _, _, proto = tmp_CONFIGS[conn_id]
            factory.disconnect_adapter(proto, adapter)