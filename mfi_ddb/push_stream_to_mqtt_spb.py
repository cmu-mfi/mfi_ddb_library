#!/usr/bin/env python3

import time

import yaml
from mqtt_spb_wrapper import MqttSpbEntityDevice
from omegaconf import OmegaConf

from mfi_ddb import BaseDataObject


class PushStreamToMqttSpb:
    """
    A class to push stream data to MQTT broker using Sparkplug B protocol.
    Attributes:
        cfg (OmegaConf): Configuration object loaded from the provided YAML file.
        data_obj (object): Data object containing component IDs, attributes, and data.
        components (dict): Dictionary to store MqttSpbEntityDevice instances for each component.
    Methods:
        __init__(cfg_file, data_obj): Initializes the PushStreamToMqttSpb instance with configuration and data object.
        connect(): Connects to the MQTT broker and initializes MqttSpbEntityDevice instances for each component.
        publish_birth(): Publishes birth certificates for all components to the MQTT broker.
        streamdata(): Streams data for all components to the MQTT broker.
    """
    def __init__(self, cfg_file, data_obj: BaseDataObject) -> None:      
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
        
        components = self.components.copy()
        for component_id in components.keys():
            # set attributes value
            attributes = self.data_obj.attributes[component_id]
            input_values = self.data_obj.data[component_id]

            if 'experiment_class' not in attributes.keys():
                if 'experiment_class' not in self.cfg.keys():
                    print(f"Experiment class not found for component {component_id}")
                    self.components.pop(component_id)
                    self.data_obj.component_ids.remove(component_id)
                    self.data_obj.attributes.pop(component_id)
                    self.data_obj.data.pop(component_id)
                    continue
                else:
                    attributes['experiment_class'] = self.cfg['experiment_class']           
            
            if not bool(input_values):
                print(f"Data not found for component {component_id}")
                self.components.pop(component_id)
                self.data_obj.component_ids.remove(component_id)
                self.data_obj.attributes.pop(component_id)
                self.data_obj.data.pop(component_id)
                continue

            for key in attributes.keys():
                if type(attributes[key]) not in [int, float, str, list]:
                    continue                 
                self.components[component_id].attributes.set_value(key, attributes[key])
                
            for key in input_values.keys():
                if type(input_values[key]) not in [int, float, str, list]:
                    continue                 
                self.components[component_id].data.set_value(key, input_values[key])
            self.components[component_id].publish_birth()
            print(f"Birth published for component {component_id}")
    
    def streamdata(self):  
              
        for component_id in self.components.keys():   
            
            input_values = self.data_obj.data[component_id]

            if not bool(input_values):
                print(f"Data not found for component {component_id}")
                continue
            
            for key in input_values.keys():
                if type(input_values[key]) not in [int, float, str, list]:
                    continue 
                data_item_prefix = 'DATA/'+key
                self.components[component_id].data.set_value(data_item_prefix, input_values[key])            
                
            self.components[component_id].publish_data()
            print(f"Data published for component {component_id}")
            self.data_obj.clear_data_buffer()