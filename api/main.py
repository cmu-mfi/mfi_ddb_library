from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse
from api.config_loader    import load_ref_mtconnect, load_ref_mqtt
from api.config_validator import validate_mtconnect, validate_mqtt
from api.connection_test  import test_mtconnect, test_mqtt
from api.config_schema    import FullConfig
from api.stream_wrapper   import publish_mtconnect, event_gen

# http://127.0.0.1:8001/docs
app = FastAPI(
    title="MFI DDB Streaming API",
    version="1.0.0",
    description=(
        "MFI DDB Streaming API enables:\n"
        "1. Validation of MTConnect and MQTT configurations.\n"
        "2. Reachability tests for MTConnect agents and MQTT brokers.\n"
        "3. Configuration stashing via a connect endpoint.\n"
        "4. Background publishing of MTConnect data to MQTT.\n"
        "5. Real-time streaming of MTConnect data over Server-Sent Events."
    ),
    docs_url="/docs",
    redoc_url="/redoc",
)

# Load reference configs
ref_mt = load_ref_mtconnect()
ref_mq = load_ref_mqtt()
_last_cfg = None

@app.post("/config/validate")
def api_validate(cfg: FullConfig):
    try:
        validate_mtconnect(cfg.mtconnect, ref_mt)
        validate_mqtt(cfg.mqtt, ref_mq)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"status": "validated"}

@app.post("/config/test")
def api_test(cfg: FullConfig):
    if not test_mtconnect(cfg.mtconnect):
        raise HTTPException(status_code=502, detail="MTConnect agent unreachable")
    if not test_mqtt(cfg.mqtt):
        raise HTTPException(status_code=502, detail="MQTT broker unreachable")
    return {"status": "MTConnect OK, MQTT OK"}

@app.post("/config/connect")
def api_connect(cfg: FullConfig):
    api_validate(cfg)
    api_test(cfg)
    raw = cfg.model_dump()
    raw["mtconnect"]["agent_ip"]   = str(raw["mtconnect"]["agent_ip"])
    raw["mtconnect"]["agent_url"]  = str(raw["mtconnect"]["agent_url"])
    raw["mqtt"]["broker_address"]  = str(raw["mqtt"]["broker_address"])
    global _last_cfg
    _last_cfg = raw
    return {"status": "connected"}

@app.post("/publish")
def api_publish(background_tasks: BackgroundTasks):
    if _last_cfg is None:
        raise HTTPException(status_code=400, detail="Must call /config/connect first")
    background_tasks.add_task(publish_mtconnect, _last_cfg)
    return {"status": "publishing started"}

@app.get("/stream")
def api_stream():
    if _last_cfg is None:
        raise HTTPException(status_code=400, detail="Must call /config/connect first")
    return StreamingResponse(event_gen(_last_cfg), media_type="text/event-stream")