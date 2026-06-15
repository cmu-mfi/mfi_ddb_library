import os
import queue
import shutil
import threading
import time
from pathlib import Path

import mfi_ddb
import pytest


def test_system_polling():
    dir_path = "tests/watch_dir"
    os.makedirs(dir_path, exist_ok=True)

    adapter_config = {
        "watch_dir": [dir_path],
        "buffer_size": 5,
        "wait_before_read": 1,
        "system": {
            "name": "test_system",
            "trial_id": "trial_001",
            "description": "Test system for LocalFilesDataAdapter",
        }
        
    }
    streamer_config = {
        "topic_family": "blob",
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }
    
    adapter = mfi_ddb.data_adapters.LocalFilesDataAdapter(adapter_config)
    streamer = mfi_ddb.Streamer(streamer_config, adapter)
    
    streamer.poll_and_stream_data()
    
    time.sleep(1)
    file_path = Path(dir_path) / "sample.txt"
    file_path.write_text("sample text")
    time.sleep(1)

    streamer.poll_and_stream_data()

    #wait for the streamer to finish processing before deleting the directory
    time.sleep(2)
    shutil.rmtree(dir_path)
    
    
def test_system_callback():
    
    dir_path = "tests/watch_dir"
    os.makedirs(dir_path, exist_ok=True)

    adapter_config = {
        "watch_dir": [dir_path],
        "buffer_size": 5,
        "wait_before_read": 1,
        "system": {
            "name": "test_system",
            "trial_id": "trial_002",
            "description": "Test system for LocalFilesDataAdapter",
        }
        
    }
    streamer_config = {
        "topic_family": "blob",
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE"
        }
    }
    
    adapter = mfi_ddb.data_adapters.LocalFilesDataAdapter(adapter_config)
    exec_queue = queue.Queue()
    
    def run_streamer():
        try:
            mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=True)
        except Exception as e:
            exec_queue.put(e)
            
    
    stream_thread = threading.Thread(target=run_streamer, daemon=True)
    stream_thread.start()
    
    time.sleep(2)
    
    #create a new file
    file_path = Path(dir_path) / "sample.txt"
    file_path.write_text("sample text")
    
    time.sleep(1)
    
    stream_thread.join()
        
    if not exec_queue.empty():
        raise exec_queue.get()
        
    #wait for the streamer to finish processing before deleting the directory
    time.sleep(2)
    shutil.rmtree(dir_path)
    
    
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