import json
import os
import platform
import socket
import time
from datetime import datetime

import paho.mqtt.client as paho_mqtt

from mfi_ddb.data_adapters import *
from mfi_ddb.topic_families import *
from mfi_ddb.utils.exceptions import ConfigError
from mfi_ddb.utils.script_utils import *

from ._mqtt import Mqtt
from ._mqtt_spb import MqttSpb
from .observer import Observer

TOPIC_CLIENTS = {
    "historian": ("MqttSpb", "HistorianTopicFamily"),
    "kv": ("Mqtt", "KeyValueTopicFamily"),
    "blob": ("Mqtt", "BlobTopicFamily"),
}

class Streamer(Observer):
    def __init__(self, config: dict, data_adp: BaseDataAdapter, stream_on_update:bool = False) -> None:
        super().__init__()
        
        
        # 1. initialize the data adapter and respective topic family client
        # `````````````````````````````````````````````````````````````````````````
        self.cfg = config
        topic_family_name = config['topic_family']
        
        if topic_family_name not in TOPIC_CLIENTS:
            raise ConfigError(f"Invalid topic family: {topic_family_name}")
        
        topic_family = globals()[TOPIC_CLIENTS[topic_family_name][1]]()
        self.__client = globals()[TOPIC_CLIENTS[topic_family_name][0]](config, topic_family)
        self.__data_adp = data_adp        
        self.__client.connect(data_adp.component_ids)
        self.__data_adp.get_data()
        print("WARNING: Waiting for birth data to be populated in the data adapter for all components...")
        while any(not bool(value) for value in self.__data_adp.data.values()):
            time.sleep(0.1)
            self.__data_adp.get_data()
        
        print("Birth data populated in the data adapter for all components.")

        
        # 2. initialize the key-value metadata and respective topic family client
        # `````````````````````````````````````````````````````````````````````````
        trial_id = str(self.__data_adp.cfg.get('trial_id', None))
        kv_topic_family = globals()[TOPIC_CLIENTS['kv'][1]]()
        blob_topic_family = globals()[TOPIC_CLIENTS['blob'][1]]()
        kv_client = globals()[TOPIC_CLIENTS['kv'][0]](config, kv_topic_family)
        blob_client = globals()[TOPIC_CLIENTS['blob'][0]](config, blob_topic_family)
        
        kv_payload = self.__generate_birth_kv_payload(self.__data_adp)
        blob_birth_payload = get_blob_json_payload_from_dict(data = kv_payload,
                                                             file_name = f'{trial_id}_metadata_birth.json',
                                                             trial_id = trial_id)
        blob_death_payload = get_blob_json_payload_from_dict(data = kv_payload,
                                                             file_name = f'{trial_id}_metadata_death.json',
                                                             trial_id = trial_id)        

        # 3. publish the key-value metadata birth message with initial data
        # `````````````````````````````````````````````````````````````````````````
        
        kv_client.set_death_payload("metadata", {'death': kv_payload})
        kv_client.connect(['metadata'])
        
        blob_client.set_death_payload("metadata", blob_death_payload)
        blob_client.connect(['metadata'])
        
        kv_client.stream_data({"metadata": {'birth':kv_payload}})
        blob_client.stream_data({"metadata": blob_birth_payload})
        
        # 4. publish the birth message of the data adapter        
        # `````````````````````````````````````````````````````````````````````````
        self.__client.publish_birth(self.__data_adp.attributes, self.__data_adp.data)
        self.__data_adp.clear_data_buffer()
        
        self.__last_poll_update = 0
        
        if stream_on_update:
            self.__data_adp.add_observer(self)
        
    def reconnect(self, config=None):
        config = self.cfg if config==None else config
        self.__client.disconnect()
        
        wait_time = 1
        print(f"Trying to reinitialize Data Adapter and connect in {wait_time} second...")
        time.sleep(wait_time)
        
        data_adp = self.__data_adp.__class__(self.__data_adp.cfg)
        self.__init__(config, data_adp)
    
    def poll_and_stream_data(self, polling_rate_hz: int = 1):
        
        while (time.time() - self.__last_poll_update) < 1/polling_rate_hz:
            time.sleep(0.1)            
        
        self.__data_adp.get_data()
        self.__client.stream_data(self.__data_adp.data)
        self.__data_adp.clear_data_buffer()
                
        self.__last_poll_update = time.time()
        
    def on_data_update(self, data: dict):
        self.__client.stream_data(data)
        self.__data_adp.clear_data_buffer(list(data.keys()))

    def stream_data(self):
        self.__client.stream_data(self.__data_adp.data)
        self.__data_adp.clear_data_buffer()
        
    def __generate_birth_kv_payload(self, data_adp: BaseDataAdapter) -> dict:
        
        payload = {
            "time": {
                "birth": datetime.now().isoformat()
                },
            "source": {
                "os": platform.system(),
                "hostname": socket.gethostname(),
                "fqdn": socket.getfqdn(),
            },
            "adapter": {
                "config": self.__data_adp.cfg,
                "component_ids": self.__data_adp.component_ids,
                "attributes": self.__data_adp.attributes,
                "sample_data": self.__data_adp.data,
            },
            "broker": self.__client.cfg            
        }         
        
        return payload