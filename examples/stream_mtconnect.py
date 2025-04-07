import os

import yaml

from mfi_ddb import Streamer, MTconnectDataAdapter

if __name__ == "__main__":

    current_dir = os.path.dirname(os.path.realpath(__file__))

    mtc_config_file = os.path.join(current_dir, "mtconnect.yaml")
    mqtt_config_file = os.path.join(current_dir, "mqtt.yaml")

    with open(mtc_config_file, "r") as file:
        mtc_config = yaml.load(file, Loader=yaml.FullLoader)

    with open(mqtt_config_file, "r") as file:
        mqtt_config = yaml.load(file, Loader=yaml.FullLoader)

    mtc_adapter = MTconnectDataAdapter(mtc_config)
    streamer = Streamer(mqtt_config, mtc_adapter)

    while True:
        streamer.poll_and_stream_data(mtc_config['mtconnect']['stream_rate'])
