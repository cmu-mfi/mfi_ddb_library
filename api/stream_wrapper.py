import time
import json
from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from mfi_ddb.streamer import Streamer


def publish_mtconnect(cfg: dict):
    """
    Background task: polls MTConnect and publishes via Streamer
    """
    cfg.setdefault('topic_family', 'historian')
    adapter  = MTconnectDataAdapter(cfg)
    streamer = Streamer(cfg, adapter)
    rate     = cfg['mtconnect']['stream_rate']

    while True:
        # print("⟳ polling MTConnect…")               to try and check if it's alive
        streamer.poll_and_stream_data(rate)
        time.sleep(1.0 / rate) 


def event_gen(cfg: dict):
    """
    SSE generator: polls MTConnect and yields Server-Sent Events
    """
    adapter = MTconnectDataAdapter(cfg)
    rate    = cfg["mtconnect"]["stream_rate"]

    while True:
        adapter.get_data()
        yield f"data: {json.dumps(adapter._data)}\n\n"
        time.sleep(rate)