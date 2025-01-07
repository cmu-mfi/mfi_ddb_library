"""
This example polls the ROS topics to stream data to MQTT broker.
The poll rate is determined by the stream_rate defined in ros.yaml.
The stream_rate defines two parameters:
    - time to wait per topic to receive a new message
    - time to wait before polling all topics again

While it is simple to use this method, use push based method if:
        1. High data publishing frequency
        2. High number of topics
"""

import os

import yaml

from mfi_ddb import PullStreamToMqttSpb, RosDataObject

if __name__ == "__main__":
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    
    ros_config_file = os.path.join(current_dir, 'ros.yaml')
    mqtt_config_file = os.path.join(current_dir, 'mqtt.yaml')
    
    with open(ros_config_file, 'r') as file:
        ros_config = yaml.load(file, Loader=yaml.FullLoader)
    
    data_obj = RosDataObject(ros_config)
    
    PullStreamToMqttSpb(mqtt_config_file, data_obj)