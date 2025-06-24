# api/stream_wrapper.py
import time
import json
from fastapi.responses import StreamingResponse
from mfi_ddb.data_adapters.mtconnect import MTconnectDataAdapter
from mfi_ddb.streamer import Streamer


def event_gen(cfg: dict):
    """
    SSE generator: polls MTConnect and yields Server-Sent Events
    """
    # Ensure defaults and normalize types
    cfg.setdefault('topic_family', 'historian')
    cfg['mtconnect']['agent_url'] = str(cfg['mtconnect']['agent_url']).rstrip('/') + '/'
    cfg['mtconnect']['agent_ip'] = str(cfg['mtconnect']['agent_ip'])

    # Instantiate adapter and stream
    adapter = MTconnectDataAdapter(cfg)
    rate = cfg['mtconnect']['stream_rate']
    topic_family = cfg['topic_family']

    while True:
        adapter.get_data()
        payload = json.dumps({
            'topic_family': topic_family,
            'data': adapter._data
        })
        yield f"data: {payload}\n\n"
        time.sleep(rate)


def publish_mtconnect(cfg: dict):
    """
    Background task: polls MTConnect and publishes via Streamer
    """
    # Ensure defaults and normalize types
    cfg.setdefault('topic_family', 'historian')
    cfg['mtconnect']['agent_url'] = str(cfg['mtconnect']['agent_url']).rstrip('/') + '/'
    cfg['mtconnect']['agent_ip'] = str(cfg['mtconnect']['agent_ip'])

    # Instantiate adapter and streamer
    adapter = MTconnectDataAdapter(cfg)
    streamer = Streamer(cfg, adapter)
    rate = cfg['mtconnect']['stream_rate']

    # Poll MTConnect and publish
    while True:
        streamer.poll_and_stream_data(rate)
        time.sleep(1.0 / rate)
