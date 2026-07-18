import os
import queue
import threading
import time

import mfi_ddb
import pytest

def test_system_polling():
    """Test ROS2 adapter in polling mode with mock ROS2 topics."""

    adapter_config = {
        "trial_id": "trial_001",
        "devices": {
            "device1": {
                "namespace": "",
                "rostopics": ["/chatter"],
                "attributes": {
                    "manufacturer": "RobotCorp",
                    "model": "RobotArmX",
                    "description": "A robotic arm for testing purposes."
                }
            }
        }
    }
    
    streamer_config = {
        "user": {
            "user_id": "user123",
            "domain": "ANDREW",
            "email": "user123@example.com",
            "name": "User 123"
        },        
        "mqtt": {
            "broker_address": "emqx",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }    
    
    # Start the ROS2 adapter and streamer
    adapter = mfi_ddb.data_adapters.Ros2DataAdapter(adapter_config)
    streamer = mfi_ddb.Streamer(streamer_config, adapter)
    
    # First poll - should get empty data initially
    streamer.poll_and_stream_data()
    time.sleep(2)
    
    # Second poll - data may have been populated if ROS2 topics exist
    streamer.poll_and_stream_data()
    time.sleep(2)
        
    # Checks
    assert len(adapter.data.keys()) == 1
    assert 'device1' in adapter.data
    assert adapter.data['device1'] != {}    

    # Cleanup
    adapter.disconnect()
    time.sleep(1)

def test_system_callback():
    """Test ROS2 adapter in callback mode with mock ROS2 topics."""

    adapter_config = {
        "trial_id": "trial_001",
        "devices": {
            "device1": {
                "namespace": "",
                "rostopics": ["/chatter"],
                "attributes": {
                    "manufacturer": "RobotCorp",
                    "model": "RobotArmX",
                    "description": "A robotic arm for testing purposes."
                }
            }
        }
    }
    
    streamer_config = {
        "user": {
            "user_id": "user123",
            "domain": "ANDREW",
            "email": "user123@example.com",
            "name": "User 123"
        },        
        "mqtt": {
            "broker_address": "emqx",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }    

    adapter = mfi_ddb.data_adapters.Ros2DataAdapter(adapter_config)
    exec_queue = queue.Queue()
    
    def run_streamer():
        try:
            mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=True)
        except Exception as e:
            exec_queue.put(e)
                        
    stream_thread = threading.Thread(target=run_streamer, daemon=True)
    stream_thread.start()
    
    # Allow some time for callbacks to process
    time.sleep(2)
    
    # Clean shutdown
    stream_thread.join(timeout=5)
    
    if not exec_queue.empty():
        raise exec_queue.get()
    
    # Checks
    assert len(adapter.data.keys()) == 1
    assert 'device1' in adapter.data
    assert adapter.data['device1'] == {}
    assert len(adapter._raw_data.keys()) == 1
    assert adapter._raw_data['device1'] != {}
    
    # Cleanup
    adapter.disconnect()
    time.sleep(1)

# Run tests if this file is executed directly
if __name__ == "__main__":
    print("Running tests for Ros2DataAdapter...")
    print("==================================")
    print("STARTING TEST 1: test_system_polling")
    test_system_polling()
    print("\nTEST 1 COMPLETED")
    print("==================================")
    print("STARTING TEST 2: test_system_callback")    
    test_system_callback()
    print("\nTEST 2 COMPLETED")
    print("==================================")