import requests
import threading
import paho.mqtt.client as mqtt


def test_mtconnect(cfg, timeout: int = 5) -> bool:
    try:
        base = str(cfg.agent_url).rstrip('/')
        url = url = f"{base}/current"
        r = requests.get(url, timeout=timeout)
        return r.status_code == 200
    except requests.RequestException:
        return False


def test_mqtt(cfg, timeout: int = 5) -> bool:
    ok = threading.Event()
    client = mqtt.Client()
    client.username_pw_set(cfg.username, cfg.password)
    client.on_connect = lambda c,u,f,rc: ok.set() if rc == 0 else None
    if cfg.tls_enabled:
        client.tls_set()
    client.connect_async(str(cfg.broker_address), cfg.broker_port)
    client.loop_start()
    success = ok.wait(timeout)
    client.loop_stop()
    client.disconnect()
    return success