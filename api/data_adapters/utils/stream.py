import time
import json
import threading
import asyncio
from fastapi import HTTPException, Form
from mfi_ddb.streamer import Streamer
from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from mfi_ddb.data_adapters.mqtt import MqttDataAdapter
from mfi_ddb.data_adapters.local_files import LocalFilesDataAdapter
from mfi_ddb.data_adapters.ros import RosDataAdapter

PROTOCOL_MAP = {
    'mtconnect': MTconnectDataAdapter,
    'mqtt':      MqttDataAdapter,
    'file':      LocalFilesDataAdapter,
    'ros':       RosDataAdapter,
}


async def event_stream(adapter, rate):
    """
    SSE endpoint for both MTConnect and Local Files.
    Simply polls adapter.get_data() at `rate` Hz
    and yields whatever ends up in adapter._data.
    """
    counter = 0

    # Send an immediate "connected" message so the UI flips green
    initial = {
        "message": "Stream connection established",
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": "CONNECTED",
        "counter": counter,
    }
    yield f"data: {json.dumps(initial)}\n\n"
    counter += 1

    while True:
        try:
            adapter.get_data()

            if any(adapter._data.values()):
                payload = {
                    "data": adapter._data,
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "OK",
                    "counter": counter,
                }
            else:
                payload = {
                    "message": "No data available yet",
                    "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "status": "WAITING",
                    "counter": counter,
                }

            yield f"data: {json.dumps(payload)}\n\n"
            counter += 1

        except Exception as e:
            err = {
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": "ERROR",
                "counter": counter,
            }
            yield f"data: {json.dumps(err)}\n\n"
            counter += 1

        await asyncio.sleep(1 / rate)

    # The following is an alternate SSE mode using Streamer + Observer
    # (left here in case you need it later)
    """
    loop  = asyncio.get_running_loop()
    queue = asyncio.Queue()
    cfg          = getattr(adapter, 'cfg', {})
    topic_family = cfg.get('topic_family', '')
    mqtt_cfg     = cfg.get('mqtt', {})

    streamer = Streamer(
        {'topic_family': topic_family, 'mqtt': mqtt_cfg},
        adapter,
        stream_on_update=False
    )

    def on_data(data: dict):
        loop.call_soon_threadsafe(queue.put_nowait, data)

    streamer._Streamer__observer.register(on_data)

    try:
        adapter.get_data()
        initial = getattr(adapter, '_data', None)
        loop.call_soon_threadsafe(queue.put_nowait, initial)
    except Exception as e:
        print("Error fetching initial data for SSE:", e)

    def poll_loop():
        streamer.poll_and_stream_data(polling_rate_hz=int(rate or 1))

    threading.Thread(target=poll_loop, daemon=True).start()

    counter = 0
    while True:
        data = await queue.get()
        payload = {
            'data':      data,
            'timestamp': time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'status':    'OK',
            'counter':   counter,
        }
        yield f"data: {json.dumps(payload)}\n\n"
        counter += 1
    """


async def publish_once(adapter_cfg: dict, topic_family: str):
    """
    One‐shot publish endpoint: push exactly one data update, then return.
    """
    # Instantiate the adapter with the full config dict
    adapter = MqttDataAdapter(adapter_cfg)

    # Pull a single snapshot
    adapter.get_data()
    snapshot = getattr(adapter, '_data', {})

    # Publisher for its MQTT logic
    publisher = Streamer(
        {'topic_family': topic_family, 'mqtt': adapter_cfg['mqtt']},
        adapter,
        stream_on_update=False
    )

    # Publish each component once
    for comp, payload in snapshot.items():
        publisher._publish(comp, payload)
