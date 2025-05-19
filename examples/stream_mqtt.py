import os
import time

import yaml

from mfi_ddb import MqttDataAdapter, Streamer

if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))

    mqtt_adp_config_file = os.path.join(current_dir, "mqtt_adp.yaml")
    mqtt_config_file = os.path.join(current_dir, "mqtt.yaml")

    with open(mqtt_adp_config_file, "r") as file:
        mqtt_adp_config = yaml.load(file, Loader=yaml.FullLoader)

    with open(mqtt_config_file, "r") as file:
        mqtt_config = yaml.load(file, Loader=yaml.FullLoader)

    choice = input("Choose the streaming method:\n1. Polling\n2. Callback\n")
    while choice not in ["1", "2"]:
        choice = input("Invalid choice. Choose 1 or 2:\n1. Polling\n2. Callback\n")

    mqtt_adp_adapter = MqttDataAdapter(mqtt_adp_config)
    
    # 1. polling and streaming data
    if choice == "1":
        print("Polling method selected.")
    
        streamer = Streamer(mqtt_config, mqtt_adp_adapter)
        while True:
                streamer.poll_and_stream_data()
                
    # 2. callback based streaming
    elif choice == "2":
        print("Callback method selected.")
            
        streamer = Streamer(mqtt_config, mqtt_adp_adapter, stream_on_update=True)
        mqtt_adp_adapter.start_listening()
    