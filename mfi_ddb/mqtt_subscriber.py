import base64
import json
import os
import pickle
import time

import numpy as np
import paho.mqtt.client as mqtt

class MQTTSubscriber:
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

    def __on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")

    def __on_message(self, client, userdata, message):
        print(f"Received message on topic '{message.topic}'")       
        
        data = base64.b64decode(message.payload)
        data_dict = pickle.loads(data)
        
        path = self.config['output_path']
        folder = message.topic.replace('/', '.')
        path = os.path.join(path, folder)
        if not os.path.exists(path):
            os.makedirs(path)
            
        msg_type = message.topic.split('/')[0]
        if msg_type == 'lfs':
            self.__lfs_handler(message, data_dict, path)
        else:
            print(f"Message '{msg_type}' not supported")
        
    def __lfs_handler(self, data: dict, output_path):
        filename = "untitled"
        ext = ""
        experiment_class = ''.join(np.random.choice(list('0123456789abcdefghijklmnopqrstuvwxyz'), 4))
        time_val = time.strftime("%Y-%m-%d_%H-%M")
        timestamp = f"{time_val}"

        print(f'Keys available in the message: {data.keys()}')
        
        # Appending the filename with the name, experiment_class and timestamp
        if 'name' in data:
            name = data['name']
            filename = name.split('.')[0]
            ext = name.split('.')[1]
                
        if 'experiment_class' in data:
            if data['experiment_class'] != '':
                experiment_class = data['experiment_class']
        filename += f"_{experiment_class}"
        
        if 'timestamp' in data:
            timestamp = data['timestamp']
        filename += f"_{timestamp}"

        filename += f".{ext}"        
        with open(os.path.join(output_path, filename), 'wb') as file:
            file.write(data['file'])
        
        # Saving metadata as a json file
        metadata = data
        metadata.pop('file')
        metadata = json.dumps(metadata)
        with open(os.path.join(output_path, f"{filename}_meta.json"), 'w') as file:
            file.write(metadata)        
        
        print(f"File {filename} saved to {output_path}")  