import copy
import logging
import time
# from typing import Tuple

import paho.mqtt.client as mqtt
import pytest

logger = logging.getLogger(__name__)

@pytest.fixture(scope="session")
def streamer_config():
    return {
        "user": {
            "user_id": "user123",
            "domain": "ANDREW",
            "email": "user123@example.com",
            "name": "User 123"
        },        
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }