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
    description="""
    This service lets you:
    1. Validate MTConnect & MQTT configs  
    2. Test reachability  
    3. “Connect” (stash) a config  
    4. Publish MTConnect → MQTT in background  
    5. Stream MTConnect data as Server-Sent Events
    """,
    version="1.0.0",
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
    global _last_cfg
    _last_cfg = cfg.model_dump()
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