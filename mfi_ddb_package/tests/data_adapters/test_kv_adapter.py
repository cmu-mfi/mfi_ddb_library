import os
import time

import mfi_ddb


def test_system_polling(streamer_config: dict):
    dir_path = "tests/watch_dir"
    os.makedirs(dir_path, exist_ok=True)

    adapter_config = mfi_ddb.KvDataAdapter.CONFIG_EXAMPLE

    adapter = mfi_ddb.data_adapters.KvDataAdapter(adapter_config)
    streamer = mfi_ddb.Streamer(streamer_config, adapter)

    streamer.poll_and_stream_data()

    # wait for the streamer to finish processing before deleting the directory
    time.sleep(2)
    streamer.disconnect()


# Run tests if this file is executed directly
if __name__ == "__main__":
    streamer_config = {
        "user": {
            "user_id": "user123",
            "domain": "ANDREW",
            "email": "user123@example.com",
            "name": "User 123",
        },
        "mqtt": {
            "broker_address": "localhost",
            "broker_port": 1883,
            "enterprise": "TEST_ORG",
            "site": "TEST_SITE",
        },
    }
    print("Running tests for KvDataAdapter...")
    print("==================================")
    print("STARTING TEST 1: test_system_polling")
    test_system_polling(streamer_config)
    print("\nTEST 1 COMPLETED")
    print("==================================")
