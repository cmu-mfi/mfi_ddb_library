import os

import yaml

from mfi_ddb import Streamer, LocalFilesDataAdapter

if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))

    ros_config_file = os.path.join(current_dir, "localfiles.yaml")
    mqtt_config_file = os.path.join(current_dir, "mqtt.yaml")

    with open(ros_config_file, "r") as file:
        lfs_config = yaml.load(file, Loader=yaml.FullLoader)

    with open(mqtt_config_file, "r") as file:
        mqtt_config = yaml.load(file, Loader=yaml.FullLoader)

    lfs_adapter = LocalFilesDataAdapter(lfs_config)

    choice = input("Choose the streaming method:\n1. Polling\n2. Callback\n")
    while choice not in ["1", "2"]:
        choice = input("Invalid choice. Choose 1 or 2:\n1. Polling\n2. Callback\n")
    
    # 1. polling and streaming data
    if choice == "1":
        print("Polling method selected.")
    
        streamer = Streamer(mqtt_config, lfs_adapter)
        while True:
                streamer.poll_and_stream_data()
                
    # 2. callback based streaming
    elif choice == "2":
        print("Callback method selected.")
            
        streamer = Streamer(mqtt_config, lfs_adapter, stream_on_update=True)
    