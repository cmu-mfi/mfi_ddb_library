import json
import os

import yaml

from mfi_ddb import BlobTopicFamily, Subscriber
from mfi_ddb.utils.script_utils import get_topic_from_config

def read_config():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    mqtt, cfs = (yaml.safe_load(
        open(os.path.join(current_dir, f'{name}.yaml'))) for name in ['mqtt', 'cfs'])

    return mqtt, cfs

def callback(config, message):
    data_type, data = BlobTopicFamily.process_message(message)
    print(f"Received {data_type} message with keys: {data.keys()}")

    if data_type == "attributes":
        attributes_file_name = f"{data['trial_id']}_{data['timestamp']}.json"
        attributes_file_path = os.path.join(config['save_directory'], attributes_file_name)
        with open(attributes_file_path, 'w') as file:
            json.dump(data['description'], file, indent=4)
        print(f"Attributes saved at: {attributes_file_path}")
        print("===========================\n")

    if data_type == "data":
        # SAVE DATA FILES
        expected_keys = ["file_name", "file_type", "size", "timestamp", "file"]
        if not all(key in data for key in expected_keys):
            print("WARNING: Missing keys in the received message.")
            return
        
        save_dir = config['save_directory']
        os.makedirs(save_dir, exist_ok=True)
        
        file_name = f"{data['file_name']}"
        file_path = os.path.join(save_dir, file_name)
        with open(file_path, 'wb') as file:
            file.write(data["file"])
        print(f"File saved at: {file_path}")
        
        # SAVE METADATA FILE
        data.pop("file")
        metadata_file_name = f"{data['file_name']}.json"
        metadata_file_path = os.path.join(save_dir, metadata_file_name)
        with open(metadata_file_path, 'w') as file:
            json.dump(data, file, indent=4)
        print(f"Metadata saved at: {metadata_file_path}")
        print("===========================\n")
    

if __name__ == '__main__':

    # LOAD CONFIG FILES
    mqtt_config, cfs_config = read_config()

    # INIT A CONNECTION
    mqtt_sub = Subscriber(mqtt_config)

    # SUBSCRIBE TO TOPIC AND SET A CALLBACK
    topic = get_topic_from_config(cfs_config['topic'])
    mqtt_sub.create_message_callback(
        topic, lambda message: callback(cfs_config, message))

    mqtt_sub.client.loop_start()
    
    while KeyboardInterrupt:
        pass
    
    mqtt_sub.client.loop_stop()
    mqtt_sub.client.disconnect()
