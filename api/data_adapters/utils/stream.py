import time
import json
import threading
import asyncio
from fastapi import HTTPException
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
            # Pull in any new file events
            adapter.get_data()

            # Check for any non-empty component payload
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
            # If something blows up, stream an error but keep going
            err = {
                "error": str(e),
                "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "status": "ERROR",
                "counter": counter,
            }
            yield f"data: {json.dumps(err)}\n\n"
            counter += 1

        # throttle to your configured rate
        await asyncio.sleep(1 / rate)
    """
    SSE endpoint: spins up the Streamer.poll_and_stream_data loop in
    a background thread, registers an Observer callback that pushes
    each data update into an asyncio.Queue, and then yields those
    updates to the client as SSE 'data:' messages.
    """
    loop  = asyncio.get_running_loop()
    queue = asyncio.Queue()

    # Build a minimal config for Streamer so it connects but does NOT re-publish
    cfg          = getattr(adapter, 'cfg', {})
    topic_family = cfg.get('topic_family', '')
    mqtt_cfg     = cfg.get('mqtt', {})

    # Create the Streamer with stream_on_update=False so it won't
    # re-publish to MQTT, but will still notify observers on each poll
    streamer = Streamer(
        {'topic_family': topic_family, 'mqtt': mqtt_cfg},
        adapter,
        stream_on_update=False
    )

    # Register our callback to receive observer notifications
    def on_data(data: dict):
        loop.call_soon_threadsafe(queue.put_nowait, data)

    # __observer is the Observer instance inside Streamer
    streamer._Streamer__observer.register(on_data)

    # ─── Send one initial snapshot so the UI flips to Active immediately ───
    try:
        adapter.get_data()
        initial = getattr(adapter, '_data', None)
        loop.call_soon_threadsafe(queue.put_nowait, initial)
    except Exception as e:
        print("Error fetching initial data for SSE:", e)
    # ────────────────────────────────────────────────────────────────────────

    # Start polling in a daemon thread
    def poll_loop():
        streamer.poll_and_stream_data(polling_rate_hz=int(rate or 1))

    threading.Thread(target=poll_loop, daemon=True).start()

    counter = 0
    while True:
        # Wait for the next data update
        data = await queue.get()
        payload = {
            'data':      data,
            'timestamp': time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            'status':    'OK',
            'counter':   counter,
        }
        yield f"data: {json.dumps(payload)}\n\n"
        counter += 1


async def publish_once(mqtt_cfg, topic_family):
    """
    One-shot publish endpoint: uses the existing MqttDataAdapter + Streamer
    to push a single snapshot to the broker.
    """
    mqtt_adapter = MqttDataAdapter(mqtt_cfg.dict())
    streamer      = Streamer(
        mqtt_adapter.cfg,
        mqtt_adapter,
        topic_family=topic_family
    )
    streamer.poll_and_stream_data()
