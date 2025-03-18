import os

import yaml

from mfi_ddb import Streamer, RosDataAdapter

if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))

    ros_config_file = os.path.join(current_dir, "ros.yaml")
    mqtt_config_file = os.path.join(current_dir, "mqtt.yaml")

    with open(ros_config_file, "r") as file:
        ros_config = yaml.load(file, Loader=yaml.FullLoader)

    with open(mqtt_config_file, "r") as file:
        mqtt_config = yaml.load(file, Loader=yaml.FullLoader)

    ros_adapter = RosDataAdapter(ros_config)
    streamer = Streamer(mqtt_config, ros_adapter)

    while True:
        streamer.poll_and_stream_data()
