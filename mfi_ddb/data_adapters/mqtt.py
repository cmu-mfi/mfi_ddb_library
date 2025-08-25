import re
import sys
import time

import paho.mqtt.client as mqtt

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError


class _Mqtt:
    def __init__(self, config: dict) -> None:
        super().__init__()

        self.mqtt_cfg = config        

        mqtt_keys = ['broker_address']
        if 'False' in list(map(lambda a: a in list(self.mqtt_cfg.keys()), mqtt_keys)):
            raise Exception("Config incomplete for mqtt. Following keys needed:",mqtt_keys)        
        
        self.client : mqtt.Client = None
    
    def connect(self):

        mqtt_cfg = self.mqtt_cfg
                
        # REQUIRED KEYS        
        mqtt_host = mqtt_cfg['broker_address']
        
        # OPTIONAL KEYS
        mqtt_port = int(mqtt_cfg['broker_port']) if 'broker_port' in mqtt_cfg.keys() else 1883
        mqtt_user = mqtt_cfg['username'] if 'username' in mqtt_cfg.keys() else None
        mqtt_pass = mqtt_cfg['password'] if 'password' in mqtt_cfg.keys() else None
        if 'password' in mqtt_cfg.keys():
            del self.mqtt_cfg['password']
        mqtt_tls_enabled = mqtt_cfg['tls_enabled'] if 'tls_enabled' in mqtt_cfg.keys() else False
        debug = mqtt_cfg['debug'] if 'debug' in mqtt_cfg.keys() else False
        
        self.client = mqtt.Client()
        self.client.username_pw_set(mqtt_user, mqtt_pass)
        self.client.on_connect = self.__on_connect
        self.client.connect(mqtt_host, mqtt_port, 60)
        
        while not self.client.is_connected():
            print("Connecting to MQTT broker...")
            time.sleep(1)
            self.client.loop()
                               
    def disconnect(self):
        pass
    
    def create_message_callback(self, topic, callback):
        self.client.subscribe(topic)
        
        # pass only the message to callback
        def callback_wrapper(client, userdata, message):
            callback(message)
        
        # set the callback function
        self.client.on_message = callback_wrapper
        print(f"Subscribed to topic: {topic}")
        
    def start_listening(self):
        self.client.loop_start()
        print("Connected to MQTT broker")

    def __on_connect(self, client, userdata, flags, rc):
        print(f"Connected to broker {self.mqtt_cfg['broker_address']} with result code {rc}")            


class MqttDataAdapter(BaseDataAdapter, _Mqtt):
    def __init__(self, config: dict):
        BaseDataAdapter.__init__(self, config)
        _Mqtt.__init__(self, config['mqtt'])
        
        # CONNECT TO THE MQTT BROKER
        self.connect()
        
        self.buffer_data = {}
        self.queue_size = self.cfg['queue_size'] if 'queue_size' in self.cfg.keys() else 10
        
        # CREATE CALLBACKS FOR THE TOPICS
        for topic in self.cfg['topics']:
            component_id = topic['component_id']
            topic_name = topic['topic']
            if 'trial_id' not in topic.keys():
                topic['trial_id'] = self.cfg['trial_id']
                        
            self.buffer_data[component_id] = []
            self._data[component_id] = {}
            self.attributes[component_id] = topic
            self.component_ids.append(component_id)
            
            self.create_message_callback(topic_name, self._topic_callback)
            print(f"Subscribed to topic: {topic}")
        
        self.start_listening()
        print("MqttDataAdapter initialized")
    
    def get_data(self):
        for component_id in self.component_ids:
            if len(self.buffer_data[component_id]) > 0:
                data = self.buffer_data[component_id].pop(0)
                self._data[component_id] = data
    
    def _topic_callback(self, message):
        """
        Callback function to handle incoming MQTT messages.
        """
        
        topic = message.topic
        payload = message.payload.decode('utf-8')
        payload = self.__autotype(payload)
        component_id = self.__get_component_from_topic(topic)
        print(f"Received message on topic {topic}: {payload} for component {component_id}")

        # extract key to store the message in the data dictionary
        data = {}
        subscription_topic = self.attributes[component_id]['topic']

        if subscription_topic.split('/')[-1] == '#':
            # If the topic ends with '#', it means we are subscribing to all subtopics
            # We need to extract the subtopic from the received topic
            subtopic = topic[len(subscription_topic)-1:]
            if subtopic.startswith('/'):
                subtopic = subtopic[1:]
        else:
            subtopic = 'data'
        
        if not isinstance(payload, dict):
            data[subtopic] = payload
        else:
            data = self.__extract_key_value(payload, subtopic)
        if len(self.buffer_data[component_id]) >= self.queue_size:
            self.buffer_data[component_id].pop(0)

        self.buffer_data[component_id].append(data)
        self._notify_observers({component_id: data})
                
    def __autotype(self, value):
        for cast in (int, float, eval):
            try:
                if cast is eval:
                    return eval(value.replace("true", "True").replace("false", "False"))
                else:
                    if cast is int and '.' in value:
                        return float(value)
                    return cast(value)
            except:
                continue

        return value
    
    def __extract_key_value(self, data_item, data_item_key):
        if len(data_item_key) > 0 and data_item_key[0] == '/':
            data_item_key = data_item_key[1:]
        if isinstance(data_item, dict):
            extracted_data = {}
            for key in data_item.keys():
                substitute_key = key
                extracted_data.update(self.__extract_key_value(data_item[key], f'{data_item_key}/{substitute_key}'))
            return extracted_data
        elif isinstance(data_item, list):
            extracted_data = {}
            for i, item in enumerate(data_item):
                extracted_data.update(self.__extract_key_value(item, f'{data_item_key}_{i}'))
            return extracted_data
        else:
            return {data_item_key: self.__autotype(data_item)}

    def __get_component_from_topic(self, topic_name):
        """
        Extract the component ID from the topic name.
        """
        for component_id, attr in self.attributes.items():
            pattern = re.escape(attr['topic']).replace(r'/\#', r'(?:/?.*)?')
            if re.fullmatch(pattern, topic_name):
                return component_id

        raise ConfigError(f"ERROR in file {__file__} line {sys._getframe().f_lineno}.\n Please raise an issue with the development team.")

        return None