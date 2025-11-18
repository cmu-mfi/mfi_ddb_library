import os
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

    streamer.disconnect()
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
    stream_thread = threading.Thread(target=lambda: mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=True))
    stream_thread.daemon = True
    stream_thread.start()
    
    time.sleep(2)
    
    #create a new file
    file_path = Path(dir_path) / "sample.txt"
    file_path.write_text("sample text")
    
    time.sleep(1)
    
    stream_thread.join()
    
    '''
    streamer = mfi_ddb.Streamer(streamer_config, adapter, stream_on_update=True)
    
    # wait for 2 seconds
    wait_time = 2
    start_time = time.time()
    while time.time() - start_time < wait_time:
        time.sleep(0.1)
        
    #create a new file
    file_path = Path(dir_path) / "sample.txt"
    file_path.write_text("sample text")
    
    # wait for 2 seconds
    wait_time = 5
    start_time = time.time()
    while time.time() - start_time < wait_time:
        time.sleep(0.1)
    '''
    
    shutil.rmtree(dir_path)