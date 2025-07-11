import copy
import json
import os
import time

import paho.mqtt.client as mqtt

from mfi_ddb.topic_families.base import BaseTopicFamily
from mfi_ddb.utils.exceptions import ConfigError


class Mqtt:
    def __init__(self, config: dict, topic_family: BaseTopicFamily = None) -> None:
        super().__init__()

        self.cfg = config
        
        if 'mqtt' not in self.cfg.keys():
            raise ConfigError("\'mqtt\' config required in streamer config file")
        else:
            mqtt_keys = ['enterprise', 
                        'broker_address']
            if 'False' in list(map(lambda a: a in list(self.cfg['mqtt'].keys()), mqtt_keys)):
                raise Exception("Config incomplete for mqtt. Following keys needed:",mqtt_keys)        
        
        self.client : mqtt.Client = None
        self._components: list = []
        self._topic_family = topic_family
        try:
            self.__topic_header = self.__get_topic_header(config['mqtt'])
        except:
            self.__topic_header = None
        
        self.__last_will_set = False
        self.__last_will = {}
    
    def __get_topic_header(self, config:dict):
        ver = 'mfi-v1.0'
        topic_family = self._topic_family.topic_family_name
        topic_head = '-'.join([ver, topic_family])
        
        enterprise = config['enterprise'] if 'enterprise' in config.keys() else ""
        site = config['site'] if 'site' in config.keys() else ""
        area = config['area'] if 'area' in config.keys() else ""
        
        args = [topic_head, enterprise, site, area]
        return '/'.join([arg for arg in args if arg])

    def connect(self, component_ids:list = []):

        mqtt_cfg = self.cfg['mqtt']
        
        # REQUIRED KEYS        
        mqtt_host = mqtt_cfg['broker_address']
        
        # OPTIONAL KEYS
        mqtt_port = int(mqtt_cfg['broker_port']) if 'broker_port' in mqtt_cfg.keys() else 1883
        mqtt_user = mqtt_cfg['username'] if 'username' in mqtt_cfg.keys() else None
        mqtt_pass = mqtt_cfg['password'] if 'password' in mqtt_cfg.keys() else None
        if 'password' in mqtt_cfg.keys():
            del self.cfg['mqtt']['password']  # Remove password from config for security reasons
            
        mqtt_tls_enabled = mqtt_cfg['tls_enabled'] if 'tls_enabled' in mqtt_cfg.keys() else False
        debug = mqtt_cfg['debug'] if 'debug' in mqtt_cfg.keys() else False
        
        self.client = mqtt.Client()
        self.client.username_pw_set(mqtt_user, mqtt_pass)
        self.client.on_connect = self.__on_connect
        
        # LAST WILL AND TESTAMENT
        self.__set_last_will()        

        self.client.connect(mqtt_host, mqtt_port, 60)
        self.client.loop_start()

        while not self.client.is_connected():
            print("Connecting to MQTT broker...")
            time.sleep(1)

        self._components = component_ids
        
    def __on_connect(self, client, userdata, flags, rc):
        print(f"All components connected to broker with result code {rc}")            
        
    def publish_birth(self, attributes, data):
        if not bool(self._components):
            #TODO: get class name str and use for error messages
            raise Exception("No component connected")
        
        # check if attributes keys and data keys are same
        if set(attributes.keys()) != set(data.keys()):
            raise Exception("Attributes and data keys are not same. Birth publish failed")
        
        for component_id in attributes.keys():
            component_attr = attributes[component_id]
            component_attr = self._topic_family.process_attr(component_attr)
            if not bool(component_attr):
                print(f"Attributes not found for device {component_id}")
                continue
            elif not self.__check_attributes(component_attr):
                raise Exception(f"{self._topic_family.topic_family_name} not compatible with Mqtt")
                      
            self.__publish(component_id, component_attr)       


        self.stream_data(data)
        
        print(f"Birth published for devices: {attributes.keys()}")
   
    def stream_data(self, data):
        for component_id in data.keys():
            input_values = data[component_id]
            input_values = self._topic_family.process_data(input_values)
            if not bool(input_values):
                print(f"WARNING: Data not found for device {component_id}")
                continue
            elif not self.__check_data(input_values):
                raise Exception(f"{self._topic_family.topic_family_name} not compatible with Mqtt")
                      
            self.__publish(component_id, input_values)       
                    
    def disconnect(self):
        self.__publish_last_will()
        self.client.loop_stop()
        print("Disconnecting from MQTT broker...")
        self.client.disconnect()
    
    def __check_data(self, data):       
        return True
    
    def __check_attributes(self, attributes):
        return True
    
    def __publish(self, device, payload: dict):
        topic_prefix = f"{self.__topic_header}/{device}"
        print(f"Publishing to device: {device}")
        for key in payload.keys():
            if isinstance(payload[key], dict):
                payload[key] = json.dumps(payload[key])
            self.client.publish(f"{topic_prefix}/{key}", payload[key])
            print(f"Published data on topic: {topic_prefix}/{key}")
            
    def set_death_payload(self, topic:str, payload: dict, qos: int = 1, retain: bool = False):
        """
        Set the last will message for the MQTT client.
        """
        
        input_values = copy.deepcopy(payload)
        input_values = self._topic_family.process_data(input_values)

        if len(input_values.keys()) != 1:
            print(f"WARNING: Death payload should have only one key. Found {len(input_values.keys())} keys.")
            return
        
        if not bool(input_values):
            print(f"WARNING: Death payload not found for {self.__topic_header}")
        elif not self.__check_data(input_values):
            raise Exception(f"{self._topic_family.topic_family_name} not compatible with Mqtt")      
            
        self.__last_will_set = True   
        topic = f"{topic}/{list(input_values.keys())[0]}"
        self.__last_will = {topic: list(input_values.values())[0]}
        
    def __publish_last_will(self):
        """
        Publish the last will message if it is set.
        """
        print(f"CLIENT DISCONNECTED. {self._topic_family.topic_family_name}. Publishing last will message...")
        if not self.__last_will_set:         
            return
        
        for device, payload in self.__last_will.items():
            topic_prefix = f"{self.__topic_header}/{device}"
            for key in payload.keys():
                if isinstance(payload[key], dict):
                    payload[key] = json.dumps(payload[key])
                self.client.publish(f"{topic_prefix}/{key}", payload[key])
                print(f"Published last will on topic: {topic_prefix}/{key}")
        self.__last_will_set = False
        self.__last_will = {}
    
    def __set_last_will(self):
        """
        Set the last will message for the MQTT client.
        """
        if not self.__last_will_set:
            return
        
        payload = list(self.__last_will.values())[0]
        topic = f"{self.__topic_header}/{list(self.__last_will.keys())[0]}"
            
        self.client.will_set(topic, payload, qos=1, retain=False)
        print(f"Last will set on topic: {topic}")