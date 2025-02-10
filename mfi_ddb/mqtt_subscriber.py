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
        broker = mqtt_cfg['broker_address']
        port = mqtt_cfg['broker_port']
        username = mqtt_cfg['username']
        password = mqtt_cfg['password']
        self.client = mqtt.Client()
        self.client.username_pw_set(username, password)
        self.client.on_connect = self.__on_connect
        self.client.on_message = self.__on_message
        self.client.connect(broker, port, 60)
        
        self.config = config
        print(config)
        
        self.msg_handlers = {}
        
        for topic in config['topics']:
            self.client.subscribe(topic)
            self.msg_handlers[topic] = []
        # self.client.subscribe("lfs/keyence")
        
        self.__subscribe_mfi_handlers()
    
    def create_msg_handler(self, msg_handler: callable, topic: str, config: dict):
        handler_dict = {
            'handler': lfs_handler,
            'config': config
        }
        self.msg_handlers[topic].append(handler_dict)
        
    def __subscribe_mfi_handlers(self):
        for topic in self.config['topics']:
            topic_type = topic.split('/')[0]
            if topic_type == 'lfs' and 'lfs_handler' in self.config:
                self.create_msg_handler(lfs_handler, topic, self.config['lfs_handler'])
                print(f"Using mfi_ddb lfs_handler for topic '{topic}'")
            else:
                print(f"Warning: Topic '{topic_type}' not supported by mfi_ddb or handler configuration not found")

    def __on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")

    def __on_message(self, client, userdata, message):
        print(f"Received message on topic '{message.topic}'")       
        # print(f"Message payload: {message.payload}") 
        return
        # data = base64.b64decode(message.payload)
        data_dict = pickle.loads(message.payload)

        for topic in self.msg_handlers:
            if self.__mqtt_topics_match(topic, message.topic):
                for handler_dict in self.msg_handlers[topic]:
                    handler = handler_dict['handler']
                    config = handler_dict['config']
                    handler(message.topic, data_dict, config)
        
    def __mqtt_topics_match(topic1, topic2):
        # Split topics by '/'
        parts1 = topic1.split('/')
        parts2 = topic2.split('/')
        
        # Check if both topics have the same number of parts or one of them has a # wildcard at the end
        if len(parts1) != len(parts2) and not ('#' in parts1 or '#' in parts2):
            return False

        # Compare each part of the topics
        for p1, p2 in zip(parts1, parts2):
            if p1 == p2:
                continue  # Exact match
            if p2 == '+':
                continue  # Wildcard for single level
            if p1 == '+':
                continue  # Wildcard for single level
            if p1 == '#' or p2 == '#':
                return True  # '#' matches everything from this point forward

            return False  # Non-matching parts and no wildcard match

        # If we have a # wildcard in either of the topics, it can match anything after that point
        return True    