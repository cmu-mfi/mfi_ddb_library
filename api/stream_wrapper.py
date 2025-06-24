# api/stream_wrapper.py
import time
import json
import paho.mqtt.client as mqtt
from fastapi.responses import StreamingResponse
from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter

def event_gen(adapter: MTconnectDataAdapter, rate: float):
    """
    Generator for SSE streaming.
    """
    while True:
        adapter.get_data()
        payload = json.dumps(adapter._data)
        yield f"data: {payload}\n\n"
        time.sleep(rate)

def stream_mtconnect(cfg: dict):
    """
    HTTP SSE endpoint: pulls from MTConnect and pushes to the client.
    """
    adapter = MTconnectDataAdapter(cfg)
    rate = cfg["mtconnect"]["stream_rate"]
    return StreamingResponse(
        event_gen(adapter, rate),
        media_type="text/event-stream"
    )

def publish_mtconnect(cfg: dict):
    """
    Background task: polls MTConnect and publishes to your MQTT broker.
    """
    adapter = MTconnectDataAdapter(cfg)

    # set up MQTT client
    client = mqtt.Client()
    if cfg["mqtt"].get("tls_enabled", False):
        client.tls_set()            # or more detailed tls config
    client.username_pw_set(
        cfg["mqtt"]["username"],
        cfg["mqtt"]["password"]
    )
    client.connect(
        cfg["mqtt"]["broker_address"],
        cfg["mqtt"].get("broker_port", 1883)
    )
    client.loop_start()

    topic_base = f"{cfg['mqtt']['enterprise']}/{cfg['mqtt']['site']}"
    rate = cfg["mtconnect"]["stream_rate"]

    # main publish loop
    while True:
        adapter.update_data()
        payload = json.dumps(adapter._data)
        client.publish(f"{topic_base}/#", payload, qos=0)
        time.sleep(1.0 / rate)
