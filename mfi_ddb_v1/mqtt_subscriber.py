import base64
import json
import os
import pickle
import time

import numpy as np
import paho.mqtt.client as mqtt
from mfi_ddb.msg_handlers.lfs_handler import lfs_handler

class MqttSubscriber:
    """
    A class to handle MQTT subscriptions and message processing.
    Attributes:
    -----------
    client(paho.mqtt.client.Client): The MQTT client instance.
    config(dict): Configuration dictionary containing topics and output path.
        Expected keys:
            - topics: List of topics to subscribe to.
            - output_path: Path to save the received files.
        
    Methods:
    --------
    __on_connect(client, userdata, flags, rc): Callback method for when the client receives a CONNACK response from the server.
    __on_message(client, userdata, message): Callback method for when a PUBLISH message is received from the server.
    __lfs_handler(data, output_path): Handles the 'lfs' type messages, saving the file and metadata to the specified output path.
    """
    def __init__(self, mqtt_cfg, config):
        broker = mqtt_cfg['broker']
        port = mqtt_cfg['port']
        username = mqtt_cfg['username']
        password = mqtt_cfg['password']
        self.client = mqtt.Client()
        self.client.username_pw_set(username, password)
        self.client.on_connect = self.__on_connect
        self.client.on_message = self.__on_message
        self.client.connect(broker, port, 60)
        
        self.config = config
        print(config)
        
        for topic in config['topics']:
            self.client.subscribe(topic)
        # self.client.subscribe("lfs/keyence")

    def __on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")

    def __on_message(self, client, userdata, message):
        print(f"Received message on topic '{message.topic}'")       
        
        # data = base64.b64decode(message.payload)
        data_dict = pickle.loads(message.payload)
        
        path = self.config['output_path']
        folder = message.topic.replace('/', '.')
        path = os.path.join(path, folder)
        if not os.path.exists(path):
            os.makedirs(path)
            
        topic_type = message.topic.split('/')[0]
        if topic_type == 'lfs':
            lfs_handler(data_dict, path)
        else:
            print(f"Topic '{topic_type}' not supported")