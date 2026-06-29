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

import logging
import time

import paho.mqtt.client as mqtt
import pytest

from pg_mds import MdsConnector

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def connector():
    return MdsConnector(config_path="./pg_database.test.ini")

@pytest.fixture(scope="session")
def client():
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
def lookup(connector):
    return connector._lookup