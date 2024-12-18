#!/usr/bin/env python3

import os
import time

import random
import yaml
from omegaconf import OmegaConf
import xmltodict

from mqtt_spb_wrapper import MqttSpbEntityDevice

class PushStreamToMqttSpb:
    def __init__(self, cfg_file, data_obj) -> None:      
        with open(cfg_file, 'r') as file:
            config = yaml.load(file, Loader=yaml.FullLoader) 
        
        cfg = OmegaConf.create(config)        
        
        self.cfg = cfg
        self.data_obj = data_obj
        self.components = {}
        
        self.connect()
        self.publish_birth()

    def connect(self):
        group_name = self.cfg['mqtt']['group_name']
        edge_node_name = self.cfg['mqtt']['node_name']
        mqtt_host = self.cfg['mqtt']['broker_address']
        mqtt_port = int(self.cfg['mqtt']['broker_port'])
        mqtt_user = self.cfg['mqtt']['username']
        mqtt_pass = self.cfg['mqtt']['password']
        mqtt_tls_enabled = self.cfg['mqtt']['tls_enabled']
        debug = self.cfg['mqtt']['debug']
        
        components = self.data_obj.component_ids
        
        for component_id in components:
            spb_component = MqttSpbEntityDevice(group_name,
                                                edge_node_name,
                                                component_id,
                                                debug)

            _connected = False
            while not _connected:
                _connected = spb_component.connect(mqtt_host,
                                                mqtt_port,
                                                mqtt_user,
                                                mqtt_pass,
                                                mqtt_tls_enabled)
                if not _connected:
                    print("  Error, could not connect. Trying again in a few seconds ...")
                    time.sleep(3)

            self.components[component_id] = spb_component
        print(f"All components connected to broker")    

    def publish_birth(self):
        
        for component_id in self.components.keys():
            # set attributes value
            experiment_class = self.cfg['experiment_class']
            self.components[component_id].attributes.set_value('experiment_class', experiment_class)
            
            attributes = self.data_obj.attributes[component_id]
            input_values = self.data_obj.data[component_id]

            for key in attributes.keys():
                self.components[component_id].attributes.set_value(key, attributes[key])
                
            for key in input_values.keys():                
                self.components[component_id].data.set_value(key, input_values[key])

            self.components[component_id].publish_birth()
            print(f"Birth published for component {component_id}")
    
    def streamdata(self):  
              
        for component_id in self.components.keys():   
            
            input_values = self.data_obj.data[component_id]

            for key in input_values.keys():
                data_item_prefix = 'DATA/'+key
                self.components[component_id].data.set_value(data_item_prefix, input_values[key])            
                
            self.components[component_id].publish_data()

            print(f"Data published for component {component_id}")
