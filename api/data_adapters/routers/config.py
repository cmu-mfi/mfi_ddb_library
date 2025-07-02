from fastapi import APIRouter, Path, UploadFile, File, Form, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from api.data_adapters.utils.loader import load_config
from api.data_adapters.utils.stream import event_stream, publish_once
from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from mfi_ddb.data_adapters.mqtt        import MqttDataAdapter
from mfi_ddb.data_adapters.local_files import LocalFilesDataAdapter
from mfi_ddb.data_adapters.ros         import RosDataAdapter

router = APIRouter()

ADAPTER_MAP = {
    'mtconnect': MTconnectDataAdapter,
    'mqtt':      MqttDataAdapter,
    'file':      LocalFilesDataAdapter,
    'ros':       RosDataAdapter,
}

# In‐memory registries keyed by connection ID
tmp_ADAPTERS  = {}   # id -> adapter instance
tmp_STREAMERS = {}   # id -> Streamer or dummy True
tmp_RATES     = {}   # id -> stream_rate


def pick_protocol(cfg):
    if cfg.mtconnect is not None:
        return 'mtconnect'
    if cfg.file is not None:
        return 'file'
    if cfg.watch_dir is not None and cfg.system is not None:
        return 'file'
    if cfg.mqtt is not None:
        return 'mqtt'
    if cfg.ros is not None:
        return 'ros'
    raise HTTPException(400, "No valid protocol configuration found")


@router.post('/validate')
async def validate(
    file: UploadFile = File(None),
    text: str        = Form(None)
):
    raw = (await file.read()).decode() if file else text
    cfg = await load_config(file, text)
    proto = pick_protocol(cfg)
    adapter_cfg = cfg.model_dump()

    # Local‑files nested
    if proto == 'file' and adapter_cfg.get('file') is None:
        adapter_cfg['file'] = {
            'watch_dir':        adapter_cfg.get('watch_dir'),
            'stream_rate':      adapter_cfg.get('stream_rate', 1.0),
            'wait_before_read': adapter_cfg.get('wait_before_read'),
            'buffer_size':      adapter_cfg.get('buffer_size'),
            'system':           adapter_cfg.get('system'),
        }
    # MQTT topics injection
    if proto == 'mqtt':
        m = cfg.mqtt
        sub = f"mfi-v1.0-{m.topic_family}/{m.enterprise}/{m.site}/#"
        adapter_cfg['topics']   = [ {'topic': sub, 'component_id': m.topic_family} ]
        adapter_cfg['trial_id'] = None

    try:
        ADAPTER_MAP[proto](adapter_cfg)
        return {'valid': True}
    except Exception as e:
        raise HTTPException(400, f"Invalid {proto} config: {e}")


@router.post('/test')
async def test(
    file: UploadFile = File(None),
    text: str        = Form(None)
):
    cfg = await load_config(file, text)
    proto = pick_protocol(cfg)
    adapter_cfg = cfg.model_dump()

    if proto == 'file' and adapter_cfg.get('file') is None:
        adapter_cfg['file'] = {
            'watch_dir':        adapter_cfg.get('watch_dir'),
            'stream_rate':      adapter_cfg.get('stream_rate', 1.0),
            'wait_before_read': adapter_cfg.get('wait_before_read'),
            'buffer_size':      adapter_cfg.get('buffer_size'),
            'system':           adapter_cfg.get('system'),
        }
    if proto == 'mqtt':
        m = cfg.mqtt
        sub = f"mfi-v1.0-{m.topic_family}/{m.enterprise}/{m.site}/#"
        adapter_cfg['topics']   = [ {'topic': sub, 'component_id': m.topic_family} ]
        adapter_cfg['trial_id'] = None

    adapter = ADAPTER_MAP[proto](adapter_cfg)
    ok = adapter.test_connection() if hasattr(adapter, 'test_connection') else True
    if not ok:
        raise HTTPException(502, f"{proto} connectivity test failed")
    return {proto: ok}


@router.post('/connect/{id}')
async def connect(
    id: str = Path(..., description="Connection ID"),
    file: UploadFile = File(None),
    text: str        = Form(None)
):
    cfg = await load_config(file, text)
    proto = pick_protocol(cfg)
    adapter_cfg = cfg.model_dump()

    if proto == 'file' and adapter_cfg.get('file') is None:
        adapter_cfg['file'] = {
            'watch_dir':        adapter_cfg.get('watch_dir'),
            'stream_rate':      adapter_cfg.get('stream_rate', 1.0),
            'wait_before_read': adapter_cfg.get('wait_before_read'),
            'buffer_size':      adapter_cfg.get('buffer_size'),
            'system':           adapter_cfg.get('system'),
        }
    if proto == 'mqtt':
        m = cfg.mqtt
        sub = f"mfi-v1.0-{m.topic_family}/{m.enterprise}/{m.site}/#"
        adapter_cfg['topics']   = [ {'topic': sub, 'component_id': m.topic_family} ]
        adapter_cfg['trial_id'] = None

    # instantiate adapter
    adapter = ADAPTER_MAP[proto](adapter_cfg)
    tmp_ADAPTERS[id] = adapter

    # store rate
    rate = adapter_cfg['file']['stream_rate'] if proto == 'file' else getattr(cfg, proto).stream_rate
    tmp_RATES[id] = rate

    # streaming setup
    if cfg.mqtt:
        from mfi_ddb.streamer import Streamer
        streamer = Streamer(
            {'topic_family': cfg.mqtt.topic_family, 'mqtt': cfg.mqtt.model_dump()},
            adapter,
            stream_on_update=True
        )
        tmp_STREAMERS[id] = streamer
    elif proto == 'file':
        tmp_STREAMERS[id] = True
    else:
        tmp_STREAMERS[id] = None

    return {
        'connected': True,
        'protocol': proto,
        'mqtt_streaming': bool(tmp_STREAMERS[id])
    }


@router.get('/streaming-status/{id}')
async def streaming_status(id: str = Path(...)):
    adapter = tmp_ADAPTERS.get(id)
    streamer = tmp_STREAMERS.get(id) is not None
    
    print(f"🔍 Status check for {id}:")
    print(f"  - adapter: {adapter}")
    print(f"  - streamer: {streamer}")
    print(f"  - streamer type: {type(streamer)}")
    
    return {
        'adapter_connected': id in tmp_ADAPTERS and adapter is not None,
        'is_streaming': bool(streamer),
        'stream_rate': tmp_RATES.get(id),
    }


@router.get('/stream/{id}')
async def stream(id: str = Path(...)):
    adapter = tmp_ADAPTERS.get(id)
    rate    = tmp_RATES.get(id)
    if not adapter or rate is None:
        raise HTTPException(400, "Must call /config/connect first")
    return StreamingResponse(
        event_stream(adapter, rate),
        media_type='text/event-stream'
    )


@router.post('/publish')
async def publish(
    background_tasks: BackgroundTasks,
    file: UploadFile  = File(None),
    text: str         = Form(None),
    topic_family: str = Form(...)
):
    cfg = await load_config(file, text)
    mqtt_cfg = cfg.mqtt.model_dump() if cfg.mqtt else {}
    background_tasks.add_task(publish_once, mqtt_cfg, topic_family)
    return {'published': True}