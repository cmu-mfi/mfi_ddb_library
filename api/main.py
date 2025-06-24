# api/main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse

from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from .config_loader    import load_ref_mtconnect, load_ref_mqtt
from .config_schema    import FullConfig
from .config_validator import validate_mtconnect, validate_mqtt
from .connection_test  import test_mtconnect, test_mqtt
from .stream_wrapper   import publish_mtconnect, event_gen

app      = FastAPI()
ref_mt   = load_ref_mtconnect()
ref_mq   = load_ref_mqtt()
_last_cfg = None    # stash after “/config/connect”

@app.post("/config/validate")
def validate(cfg: FullConfig):
    try:
        validate_mtconnect(cfg.mtconnect, ref_mt)
        validate_mqtt(cfg.mqtt, ref_mq)
    except ValueError as e:
        raise HTTPException(400, detail=str(e))
    return {"status": "validated"}

@app.post("/config/test")
def test(cfg: FullConfig):
    if not test_mtconnect(cfg.mtconnect):
        raise HTTPException(502, "MTConnect agent unreachable")
    if not test_mqtt(cfg.mqtt):
        raise HTTPException(502, "MQTT broker unreachable")
    return {"status": "MTConnect OK, MQTT OK"}

@app.post("/config/connect")
def connect(cfg: FullConfig):
    # re‐validate + test
    validate(cfg)
    test(cfg)
    global _last_cfg
    _last_cfg = cfg.model_dump()   # or cfg.dict()
    return {"status": "connected"}

@app.post("/publish")
def publish(background_tasks: BackgroundTasks):
    if _last_cfg is None:
        raise HTTPException(400, "Must call /config/connect first")
    background_tasks.add_task(publish_mtconnect, _last_cfg)
    return {"status": "publishing started"}

# @app.post("/stream")
# def stream():
#     if _last_cfg is None:
#         raise HTTPException(400, "Must call /config/connect first")
#     # reuse the last config for SSE
#     adapter = MTconnectDataAdapter(_last_cfg)
#     rate    = _last_cfg["mtconnect"]["stream_rate"]
#     return StreamingResponse(
#         event_gen(adapter, rate),
#         media_type="text/event-stream"
#     )


@app.get("/stream")
def stream_get():
    if _last_cfg is None:
        raise HTTPException(400, "Must call /config/connect first")
    # call event_gen with exactly one argument: the saved config
    return StreamingResponse(event_gen(_last_cfg), media_type="text/event-stream")
