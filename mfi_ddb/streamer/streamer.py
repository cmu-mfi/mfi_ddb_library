import time

import paho.mqtt.client as mqtt
from observer import Observer

from mfi_ddb.data_adapters.base import BaseDataAdapter


class Streamer(Observer):
    def __init__(self, config: dict, data_adp: BaseDataAdapter) -> None:
        super().__init__()
        
        self.cfg = config
        self.topic_family = config['topic_family']
        
        self.client = self.connect()
        
    def connect(self):
        mqtt_host = self.cfg['mqtt']['broker_address']
        mqtt_port = int(self.cfg['mqtt']['broker_port'])
        mqtt_user = self.cfg['mqtt']['username']
        mqtt_pass = self.cfg['mqtt']['password']
        group_name = self.cfg['mqtt']['group_name']
        edge_node_name = self.cfg['mqtt']['node_name']
        mqtt_tls_enabled = self.cfg['mqtt']['tls_enabled']
        debug = self.cfg['mqtt']['debug']
                
        client = mqtt.Client()
        client.username_pw_set(mqtt_user, mqtt_pass)
        client.on_connect = self.__on_connect
        client.connect(mqtt_host, mqtt_port, 60)
        
        while not self.client.is_connected():
            print("Connecting to MQTT broker...")
            time.sleep(1)
            self.client.loop()   
            
        return client     
        