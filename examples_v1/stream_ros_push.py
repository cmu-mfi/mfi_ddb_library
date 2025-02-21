"""
This example demonstrates how to use ROS subscriber callbacks to stream data to MQTT broker.

PushStreamToMqttSpb is used to push data to MQTT broker.
RosDataObject initializes the data for PushStreamToMqttSpb object to stream data.
RosCallback is used to subscribe to ROS topics, which updates the data in RosDataObject and stream using PushStreamToMqttSpb.
"""

import os

import yaml

from mfi_ddb import PushStreamToMqttSpb, RosCallback, RosDataObject

if __name__ == "__main__":
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    
    ros_config_file = os.path.join(current_dir, 'ros.yaml')
    mqtt_config_file = os.path.join(current_dir, 'mqtt.yaml')
    
    with open(ros_config_file, 'r') as file:
        config = yaml.load(file, Loader=yaml.FullLoader) 
    
    data_obj = RosDataObject(config, enable_topic_polling=False)
    data_obj.get_data()
    mqtt_stream = PushStreamToMqttSpb(mqtt_config_file, data_obj)
    ros_callback = RosCallback(mqtt_stream)