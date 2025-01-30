#!/usr/bin/env python3

import base64
import os
import pickle
import time

import paho.mqtt.client as mqtt
import yaml
from omegaconf import OmegaConf

from mfi_ddb import BaseDataObject


class PushStreamToMqtt:
    
    def __init__(self, cfg_file, data_obj: BaseDataObject) -> None:      
        with open(cfg_file, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader) 
        
        cfg = OmegaConf.create(config)        
        
        self.cfg = cfg
        self.data_obj = data_obj
        self.topics = self.data_obj.component_ids
        
        self.connect()
        self.publish_birth()

    def connect(self):
        mqtt_host = self.cfg['mqtt']['broker_address']
        mqtt_port = int(self.cfg['mqtt']['broker_port'])
        mqtt_user = self.cfg['mqtt']['username']
        mqtt_pass = self.cfg['mqtt']['password']
        group_name = self.cfg['mqtt']['group_name']
        edge_node_name = self.cfg['mqtt']['node_name']
        mqtt_tls_enabled = self.cfg['mqtt']['tls_enabled']
        debug = self.cfg['mqtt']['debug']
                
        self.client = mqtt.Client()
        self.client.username_pw_set(mqtt_user, mqtt_pass)
        self.client.on_connect = self.__on_connect
        self.client.connect(mqtt_host, mqtt_port, 60)
        
        while not self.client.is_connected():
            print("Connecting to MQTT broker...")
            time.sleep(1)
            self.client.loop()
    
    def __on_connect(self, client, userdata, flags, rc):
        print(f"Connected with result code {rc}")
    
    def publish_birth(self):
        topics = self.topics.copy()
        for topic in topics:
            # set attributes value
            attributes = self.data_obj.attributes[topic]
            data = self.data_obj.data[topic]

            if 'experiment_class' not in attributes.keys():
                if 'experiment_class' not in self.cfg.keys():
                    print(f"Experiment class not found for component {topic}")
                    self.topics.pop(topic)
                    self.data_obj.component_ids.remove(topic)
                    self.data_obj.attributes.pop(topic)
                    self.data_obj.data.pop(topic)
                    continue
                else:
                    attributes['experiment_class'] = self.cfg['experiment_class']           
            
            if not bool(data):
                print(f"Data not found for component {topic}")
                self.topics.pop(topic)
                self.data_obj.component_ids.remove(topic)
                self.data_obj.attributes.pop(topic)
                self.data_obj.data.pop(topic)
                continue

            self.__publish(topic, {'attributes': attributes})
            self.__publish(topic, {'data': data})
            print(f"Birth published for topic {topic}")
    
    def __publish(self, topic, payload: dict):
        print(f"Publishing to topic {topic}")
        # encoded_data = base64.b64encode(payload)
        self.client.publish(topic, pickle.dumps(payload))
    
    def streamdata(self):  
        for topic in self.topics:   
            data = self.data_obj.data[topic]
            
            if not bool(data):
                print(f"Data not found for topic {topic}")
                continue
            
            self.__publish(topic, {'data': data})
            print(f"Data published for topic {topic}")