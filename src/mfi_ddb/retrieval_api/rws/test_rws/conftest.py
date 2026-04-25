
"""
RUN WITH PYTHONPATH=./metadata_store_pg
```
$ PYTHONPATH=./metadata_store_pg pytest -v tests/test_mds_connector
```

This test assumes:
* A test MQTT broker is running with configuration in
  metadata_store_pg/broker.test.ini
* The connector is running with the above test broker,
  and a test postgres database with configuration in
  metadata_store_pg/pg_database.test.ini

The above instances are configured in docker-compose.test.yml.
Make sure to run the dockers before running these tests.
"""

import copy
import logging
import time
from typing import Tuple

import paho.mqtt.client as mqtt
import pytest
from fastapi.testclient import TestClient
from helpers import get_payload_template, grace_wait, now_iso, publish, uid

try:
    from main import app
except ModuleNotFoundError:
    raise Exception("RUN WITH PYTHONPATH=./rws")    

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def rws_client():
    with TestClient(app) as c:
        yield c

@pytest.fixture(scope="session")
def mqtt_client():
    mqtt_cfg = {"broker_address": "localhost", "broker_port": 2883}

    # REQUIRED KEYS
    mqtt_host = mqtt_cfg["broker_address"]
    mqtt_port = (
        int(mqtt_cfg["broker_port"]) if "broker_port" in mqtt_cfg.keys() else 1883
    )

    def yeah_connected(client, userdata, flags, rc):
        logger.debug("MQTT CLIENT CONNECTED!")

    client = mqtt.Client()
    client.on_connect = yeah_connected

    client.connect(host=mqtt_host, port=mqtt_port, keepalive=60)
    client.loop_start()

    while not client.is_connected():
        logger.debug("Trying to connect to the MQTT broker...")
        time.sleep(1)

    yield client

    client.loop_stop()
    client.disconnect()
    
@pytest.fixture()
def user_factory(mqtt_client):
    def _make():
        # PUBLISH DATA
        payload = {
            "schema_version": "mfi-v1.0",
            "msg_type": "user",
            "user_id": uid("neo_"),
            "domain": "matrix",
            "created_by_user_id": "superadmin",
            "created_by_domain": "superadmin",
            "timestamp": now_iso()
        }
        
        publish(mqtt_client, payload)
        grace_wait()
        return (payload["user_id"], payload["domain"])
    return _make
    
@pytest.fixture()
def test_project(mqtt_client):
    payload = {
        "schema_version": "mfi-v1.0",
        "msg_type": "project",
        "project_name": uid("Artemis "),
        "created_by_user_id": "superadmin",
        "created_by_domain": "superadmin",
        "timestamp": now_iso()        
    }
    
    publish(mqtt_client, payload)
    grace_wait()
    return payload["project_name"]    

@pytest.fixture()
def trial_factory(mqtt_client, user_factory):
    def _insert_trial(user: tuple[str, str] = user_factory(), duration: float=0.2, trial_name: str = "", project_name: str = "") -> str:
        if trial_name == "":
            trial_name = uid("test_")
        birth_payload = get_payload_template("birth")
        birth_payload["trial_id"] = trial_name
        birth_payload["time"]["birth"] = now_iso()
        birth_payload["user"]["user_id"] = user[0]
        birth_payload["user"]["domain"] = user[1]
        
        if project_name != "" and project_name is not None:
            birth_payload["project"]["name"] = project_name

        publish(mqtt_client, birth_payload)
        time.sleep(duration)

        death_payload = copy.deepcopy(birth_payload)    
        death_payload["msg_type"] = "death"
        death_payload["time"]["death"] = now_iso()
        
        publish(mqtt_client, death_payload)    
        grace_wait()
        
        return birth_payload["trial_id"]
    return _insert_trial

@pytest.fixture()
def superuser():
    return ("superadmin", "superadmin")