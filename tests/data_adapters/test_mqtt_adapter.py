import os
import shutil
import threading
import time
from pathlib import Path

import paho.mqtt.client as mqtt
import json

import mfi_ddb
import pytest
import queue


def test_system_polling():
    dir_path = "tests/watch_dir"
    os.makedirs(dir_path, exist_ok=True)

    adapter_config = {
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "topics": [
            {
                "component_id": "device-1",
                "topic": "devices/1/sensor-1/data",
            },
            {
                "component_id": "device-2",
                "topic": "devices/2/#",
            }
        ]       
    }
    streamer_config = {
        "topic_family": "historian",
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }
    
    client = mqtt.Client()
    client.connect("localhost", 1883, 60)
    while not client.is_connected():
        time.sleep(0.1)
        client.loop()    
    
    def publish_birth_data():
        payload = json.dumps({"status": "online"})
        
        while client.is_connected():
            client.publish(topic = "devices/1/sensor-1/data", 
                        payload = payload)
            client.publish(topic = "devices/2/sensor-2/data", 
                        payload = payload)        
            time.sleep(1)
    
    publish_thread = threading.Thread(target=publish_birth_data, daemon=True)
    publish_thread.start()
    
    adapter = mfi_ddb.data_adapters.MqttDataAdapter(adapter_config)
    streamer = mfi_ddb.Streamer(streamer_config, adapter)
    
        
    streamer.poll_and_stream_data()

    # Publish an MQTT message to trigger polling
    
    # PUBLISH TEST MESSAGE 1    
    payload_str = json.dumps({"value": 42})
    client.publish(topic = "devices/1/sensor-1/data", 
                   payload = payload_str)

    streamer.poll_and_stream_data()
    
    # PUBLISH TEST MESSAGE 2    
    payload_str = json.dumps({"room-1": {"temp": 70}, "room-2": {"temp": 55.5}})
    client.publish(topic = "devices/2/sensor-2/data", 
                   payload = payload_str)
    payload_str = json.dumps({"room-1": {"humidity": 40}, "room-2": {"humidity": 30}})
    client.publish(topic = "devices/2/sensor-3/data", 
                   payload = payload_str)

    streamer.poll_and_stream_data()
    

def test_system_callback():
    
    dir_path = "tests/watch_dir"
    os.makedirs(dir_path, exist_ok=True)

    adapter_config = {
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "topics": [
            {
                "component_id": "device-1",
                "topic": "devices/1/sensor-1/data",
            },
            {
                "component_id": "device-2",
                "topic": "devices/2/#",
            }
        ]       
    }
    streamer_config = {
        "topic_family": "historian",
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }

    client = mqtt.Client()
    client.connect("localhost", 1883, 60)
    while not client.is_connected():
        time.sleep(0.1)
        client.loop()    
    
    def publish_birth_data():
        payload = json.dumps({"status": "online"})
        
        while client.is_connected():
            client.publish(topic = "devices/1/sensor-1/data", 
                        payload = payload)
            client.publish(topic = "devices/2/sensor-2/data", 
                        payload = payload)        
            time.sleep(1)
    
    publish_thread = threading.Thread(target=publish_birth_data, daemon=True)
    publish_thread.start()

    adapter = mfi_ddb.data_adapters.MqttDataAdapter(adapter_config)
    exec_queue = queue.Queue()
    
    def run_streamer():
        try:
            mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=True)
        except Exception as e:
            exec_queue.put(e)
                        
    stream_thread = threading.Thread(target=run_streamer, daemon=True)
    stream_thread.start()
    
    time.sleep(2)
    
    # Publish an MQTT message to trigger polling
    
    # PUBLISH TEST MESSAGE 1    
    payload_str = json.dumps({"value": 42})
    client.publish(topic = "devices/1/sensor-1/data", 
                   payload = payload_str)
    
    # PUBLISH TEST MESSAGE 2    
    payload_str = json.dumps({"room-1": {"temp": 70}, "room-2": {"temp": 55.5}})
    client.publish(topic = "devices/2/sensor-2/data", 
                   payload = payload_str)
    payload_str = json.dumps({"room-1": {"humidity": 40}, "room-2": {"humidity": 30}})
    client.publish(topic = "devices/2/sensor-3/data", 
                   payload = payload_str)
    
    time.sleep(1)
    
    stream_thread.join()
    
# Run tests if this file is executed directly
if __name__ == "__main__":
    print("Running tests for LocalFilesDataAdapter...")
    print("==================================")
    print("STARTING TEST 1: test_system_polling")
    test_system_polling()
    print("\nTEST 1 COMPLETED")
    print("==================================")
    print("STARTING TEST 2: test_system_callback")    
    test_system_callback()
    print("\nTEST 2 COMPLETED")
    print("==================================")