import os
import signal
import time

import yaml

from mfi_ddb import RosDataAdapter, Streamer

if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))

    ros_config_file = os.path.join(current_dir, "ros.yaml")
    mqtt_config_file = os.path.join(current_dir, "mqtt.yaml")

    with open(ros_config_file, "r") as file:
        ros_config = yaml.load(file, Loader=yaml.FullLoader)

    with open(mqtt_config_file, "r") as file:
        mqtt_config = yaml.load(file, Loader=yaml.FullLoader)

    ros_adapter = RosDataAdapter(ros_config)
    choice = input("Choose the streaming method:\n1. Polling\n2. Callback\n")
    while choice not in ["1", "2"]:
        choice = input("Invalid choice. Choose 1 or 2:\n1. Polling\n2. Callback\n")

    is_running = True
    def signal_handler(sig, frame):
        global is_running
        is_running = False
        print("\nShutting down...")
        exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)

    if choice == "1":
        print("Polling method selected.")

        streamer = Streamer(mqtt_config, ros_adapter)
        while is_running:
            streamer.poll_and_stream_data()
            
    elif choice == "2":
        print("Callback method selected.")

        streamer = Streamer(mqtt_config, ros_adapter, stream_on_update=True)
        is_running = True

        while is_running:
            time.sleep(1)
        