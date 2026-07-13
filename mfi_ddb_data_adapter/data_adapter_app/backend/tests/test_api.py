from fastapi.testclient import TestClient
from app.main import app
import yaml
import json
import time

from mfi_ddb.data_adapters import mqtt

client = TestClient(app)

def test_get_adapters():
    response = client.get("/connections/adapters")
    assert response.status_code == 200
    assert isinstance(response.json(), list)


def test_adapter_config_validation():
    # =======================================================
    # VALID CONFIGS
    # =======================================================
    local_files_config = {
        "watch_dir": ["/path/to/watch/dir"],
        "buffer_size": 10,
        "wait_before_read": 2,
        "system": {
            "name": "local_files_system",
            "trial_id": "trial_001",
            "description": "Local files data adapter system",
            "manufacturer": "Example Corp",
            "model": "LocalFilesModel"
        }
    }
    local_files_config_text = yaml.dump(local_files_config)
    
    mtconnect_config = {
        "mtconnect": {
            "agent_ip": "192.168.1.1",
            "agent_url": "http://192.168.1.1:5000",
            "device_name": "MTConnectDevice",
            "trial_id": "trial_001"
        }
    }
    mtconnect_config_text = yaml.dump(mtconnect_config)
    
    response_local = client.post("/connections/validate/adapter", 
        data={"adapter_name": "Local Files", "text": local_files_config_text}
    )
    assert response_local.status_code == 200
    assert response_local.json().get("is_valid") is True
    
    response_mtconnect = client.post("/connections/validate/adapter", 
        data={"adapter_name": "MTConnect", "text": mtconnect_config_text}
    )
    assert response_mtconnect.status_code == 200
    assert response_mtconnect.json().get("is_valid") is True

    # =======================================================
    # INVALID CONFIGS
    # =======================================================

    local_files_config = {
        "watch_dir": ["/path/to/watch/dir"],
        "buffer_size": 10,
        "wait_before_read": 2,
        "system": {
            "name": "local_files_system",
            "trial_id": 12345,  # INVALID TYPE. SHOULD BE STRING
            "description": "Local files data adapter system",
            "manufacturer": "Example Corp",
            "model": "LocalFilesModel"
        }
    }
    local_files_config_text = yaml.dump(local_files_config)
    
    mtconnect_config = {
        "mtconnect": {
            # AGENT IP MISSING
            "agent_url": "http://192.168.1.1:5000",
            "device_name": "MTConnectDevice",
            "trial_id": "trial_001"
        }
    }
    mtconnect_config_text = yaml.dump(mtconnect_config)
    
    response_local = client.post("/connections/validate/adapter", 
        data={"adapter_name": "Local Files", "text": local_files_config_text}
    )
    assert response_local.status_code == 200
    assert response_local.json().get("is_valid") is False
    
    response_mtconnect = client.post("/connections/validate/adapter", 
        data={"adapter_name": "MTConnect", "text": mtconnect_config_text}
    )
    assert response_mtconnect.status_code == 200
    assert response_mtconnect.json().get("is_valid") is False

            
def test_streamer_config_validation():
    # =======================================================
    # VALID CONFIGS
    # =======================================================
    streamer_config = {
        "topic_family": "blob",
        "mqtt": {
            "broker_address": "test.mosquitto.org",
            "broker_port": 1883,
            "enterprise": "CMU",
            "site": "Machine Shop",
            "tls_enabled": False,
            "debug": False,
        }
    }
    streamer_config_text = yaml.dump(streamer_config)
    
    response_streamer = client.post("/connections/validate/streamer", 
        data={"text": streamer_config_text}
    )
    assert response_streamer.status_code == 200
    assert response_streamer.json().get("is_valid") is True

    # =======================================================
    # INVALID CONFIGS
    # =======================================================
    streamer_config = {
        "topic_family": "blob",
        "mqtt": {
            "broker_address": "test.mosquitto.org",
            "broker_port": 1883,
            # "enterprise": "CMU", # MISSING REQUIRED FIELD
            "site": "Machine Shop",
            "username": "mqtt_user",
            "password": "mqtt_password",
            "tls_enabled": False,
            "debug": False,
        }
    }
    streamer_config_text = yaml.dump(streamer_config)
    
    response_streamer = client.post("/connections/validate/streamer", 
        data={"text": streamer_config_text}
    )
    assert response_streamer.status_code == 200
    assert response_streamer.json().get("is_valid") is False

    
def test_single_adapter_connection():
    # =======================================================
    # TESTING MQTT DATA ADAPTER CONNECTION
    # =======================================================    
    adapter_cfg = {
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
    adapter_cfg_text = yaml.dump(adapter_cfg)
    
    streamer_config = {
        # "topic_family": "historian",
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "CMU",
            "site": "Machine Shop",
        }
    }
    streamer_config_text = yaml.dump(streamer_config)
    
    # =======================================================
    # VALIDATE CONFIGS
    # =======================================================
    adapter_validate = client.post("/connections/validate/adapter", 
        data={"adapter_name": "MQTT", "text": adapter_cfg_text}
    )
    assert adapter_validate.status_code == 200
    assert adapter_validate.json().get("is_valid") is True
    
    streamer_validate = client.post("/connections/validate/streamer",
        data={"text": streamer_config_text}
    )
    assert streamer_validate.status_code == 200
    assert streamer_validate.json().get("is_valid") is True
    
    # =======================================================
    # STREAM BIRTH DATA
    # =======================================================
    import paho.mqtt.client as mqtt
    import threading
    
    mqtt_client = mqtt.Client()
    mqtt_client.connect("localhost", 1883, 60)
    while not mqtt_client.is_connected():
        time.sleep(0.1)
        mqtt_client.loop()    
    
    def publish_birth_data():
        payload = json.dumps({"status": "online"})
        
        while mqtt_client.is_connected():
            mqtt_client.publish(topic = "devices/1/sensor-1/data", 
                        payload = payload)
            mqtt_client.publish(topic = "devices/2/sensor-2/data", 
                        payload = payload)        
            time.sleep(1)   
    publish_thread = threading.Thread(target=publish_birth_data, daemon=True)
    publish_thread.start()
    
    # =======================================================
    # CONNECT THE ADAPTER
    # =======================================================           
    
    response_connect = client.post("/connections/connect/100", 
        data={
            "adapter_name": "MQTT",
            "adapter_text": adapter_cfg_text,
            "streamer_text": streamer_config_text,
            "is_polling": "true",
            "polling_rate_hz": 1.0
        }
    )

    assert response_connect.json().get("is_connected")
    assert response_connect.json().get("is_streaming")
    
    # =======================================================
    # DISCONNECT THE ADAPTER
    # =======================================================
    response_disconnect = client.post("/connections/disconnect/100")
    assert response_disconnect.status_code == 200
    assert response_disconnect.json().get("disconnected")
    
    
def test_multi_adapter_connection():
    # =======================================================
    # TESTING MQTT DATA ADAPTER CONNECTION
    # =======================================================    
    adapter_cfg = {
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
    adapter_cfg_text = yaml.dump(adapter_cfg)
    
    streamer_config = {
        "topic_family": "historian",
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "CMU",
            "site": "Machine Shop",
        }
    }
    streamer_config_text = yaml.dump(streamer_config)
    
    # =======================================================
    # VALIDATE CONFIGS
    # =======================================================
    adapter_validate = client.post("/connections/validate/adapter", 
        data={"adapter_name": "MQTT", "text": adapter_cfg_text}
    )
    assert adapter_validate.status_code == 200
    assert adapter_validate.json().get("is_valid") is True
    
    streamer_validate = client.post("/connections/validate/streamer",
        data={"text": streamer_config_text}
    )
    assert streamer_validate.status_code == 200
    assert streamer_validate.json().get("is_valid") is True
    
    # =======================================================
    # STREAM BIRTH DATA
    # =======================================================
    import paho.mqtt.client as mqtt
    import threading
    
    mqtt_client = mqtt.Client()
    mqtt_client.connect("localhost", 1883, 60)
    while not mqtt_client.is_connected():
        time.sleep(0.1)
        mqtt_client.loop()    
    
    def publish_birth_data():
        payload = json.dumps({"status": "online"})
        
        while mqtt_client.is_connected():
            mqtt_client.publish(topic = "devices/1/sensor-1/data", 
                        payload = payload)
            mqtt_client.publish(topic = "devices/2/sensor-2/data", 
                        payload = payload)        
            time.sleep(1)   
    publish_thread = threading.Thread(target=publish_birth_data, daemon=True)
    publish_thread.start()
    
    # =======================================================
    # CONNECT THE ADAPTER MULTIPLE TIMES AS SEPARATE INSTANCES
    # =======================================================           
    
    for i in range(3):
        connection_id = 100 + i
        response_connect = client.post(f"/connections/connect/{connection_id}", 
            data={
                "adapter_name": "MQTT",
                "adapter_text": adapter_cfg_text,
                "streamer_text": streamer_config_text,
                "is_polling": "true",
                "polling_rate_hz": 1.0
            }
        )
        assert response_connect.status_code == 200
        assert response_connect.json().get("is_connected")
        assert response_connect.json().get("is_streaming")
    
    # =======================================================
    # COUNT ACTIVE CONNECTIONS AND CHECK HEALTH
    # =======================================================
    pause_response = client.post("/connections/pause/101")
    assert pause_response.status_code == 200

    count_response = client.get("/connections/health")
    active_connections = count_response.json().get("active_connections")
    streaming_connections = count_response.json().get("streaming_connections")
    assert active_connections == 3
    assert streaming_connections == 2
    
    # =======================================================
    # DISCONNECT THE ADAPTER
    # =======================================================
    response_disconnect = client.post("/connections/disconnect/100")
    assert response_disconnect.status_code == 200
    assert response_disconnect.json().get("disconnected")
    
        
if __name__ == "__main__":
    print("\nTEST 1: Get Available Data Adapters")
    print("================================================")
    test_get_adapters()
    
    print("\nTEST 2: Validate Data Adapter Configurations")
    print("================================================")
    test_adapter_config_validation()
    
    print("\nTEST 3: Validate Streamer Configuration")
    print("================================================")
    test_streamer_config_validation()
    
    print("\nTEST 4: Test Data Adapter Connections")
    print("================================================")
    test_single_adapter_connection()
    
    print("\nTEST 5: Test Multiple Data Adapter Connections")
    print("================================================")
    test_multi_adapter_connection()

    print("================================================")
    print("All tests passed.")
    print("================================================")
    
    