import copy
import json
import os
import platform
import socket
import sys
import time
from datetime import datetime

import paho.mqtt.client as paho_mqtt
from pydantic import BaseModel, Field

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

    CONFIG_EXAMPLE = None
    CONFIG_HELP = None
    
    class SCHEMA(BaseModel):
        """
        Schema for the data adapter configuration.
        """
        # Define the schema for the data adapter configuration here
        pass
        
    def __init__(self, config: dict, data_adp: BaseDataAdapter, stream_on_update:bool = False) -> None:
        super().__init__()
        
        
        # 1. initialize the data adapter and respective topic family client
        # `````````````````````````````````````````````````````````````````````````
        self.cfg = copy.deepcopy(config)
        topic_family_name = config['topic_family']
        
        if topic_family_name not in TOPIC_CLIENTS:
            raise ConfigError(f"Invalid topic family: {topic_family_name}")
        
        topic_family = globals()[TOPIC_CLIENTS[topic_family_name][1]]()
        self.__client = globals()[TOPIC_CLIENTS[topic_family_name][0]](self.cfg, topic_family)
        self.__data_adp = data_adp        

        self.__data_adp.get_data()
        print("WARNING: Waiting for birth data to be populated in the data adapter for all components...")
        while any(not bool(value) for value in self.__data_adp.data.values()):
            time.sleep(0.1)
            self.__data_adp.get_data()
        
        self.__birth_data = copy.deepcopy(self.__data_adp.data)
        
        print("Birth data populated in the data adapter for all components.")

        self.__reset_stream()
        
        self.__last_poll_update = 0
        
        if stream_on_update:
            if(self.__data_adp.SELF_UPDATE):
                self.__data_adp.add_observer(self)
            else:
                raise Exception("Data adapter does not support self update notifications.")

    def disconnect(self):
        try:
            self.__client.disconnect()
            self.__data_adp.disconnect()
            print("Client disconnected and data adapter deleted.")
            print("Streamer instance deleted. Bye! \u2764\uFE0F  MFI")
        except Exception as e:
            print(f"Error while disconnecting: {e}")
        
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
        if self.__refresh_birth_data_with_new_keys(data):
            self.__reset_stream()        
        self.__client.stream_data(data)
        self.__data_adp.clear_data_buffer(list(data.keys()))

    def stream_data(self):
        if self.__refresh_birth_data_with_new_keys():
            self.__reset_stream()
        self.__client.stream_data(self.__data_adp.data)
        self.__data_adp.clear_data_buffer()
        
    def __generate_birth_kv_payload(self) -> dict:
        
        sample_data = copy.deepcopy(self.__birth_data)
        sample_data_size = sys.getsizeof(str(sample_data))
        
        # drop keys until the size is less than 32768 bytes
        # since total size of payload <= 65536 bytes
        # ref: error with paho-mqtt: "struct.error: 'H' format requires 0 <= number <= 65535"
        while sample_data_size > 32768:
            # remove the last key
            sample_data.popitem()
            sample_data_size = sys.getsizeof(str(sample_data))
        
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
                "sample_data": sample_data,
            },
            "broker": self.__client.cfg            
        }         
        
        return payload

    def __refresh_birth_data_with_new_keys(self, data: dict = {}) -> bool:
        """
        Check for new keys in incoming data that are not present in the stored birth data.

        Returns:
            bool: True if one or more new keys were detected and the birth data was updated;
                  False if no new keys were found.

        Side effects:
            Updates the object's stored birth data in-place to include newly discovered keys.
            
        """
        if not data:
            data = self.__data_adp.data
        
        new_key_detected = False
        for key in data.keys():
            if key not in self.__birth_data:
                print(f"WARNING: New component_id detected in data adapter: {key}.")
                '''
                Not updating the birth data structure here as it requires 
                updating the data adapter attributes as well.
                If this warning is seen, one of two scenarios is possible:
                * the data adapter is populating data for an unknown component(s).
                * or the data adapter has a bug.
                Please raise an issue on the MFI DDB GitHub repository for support.
                '''
                continue
            for sub_key in data[key].keys():
                if sub_key not in self.__birth_data[key]:
                    print(f"New data key detected in data adapter for component {key}: {sub_key}. Updating birth data.")
                    self.__birth_data[key][sub_key] = copy.deepcopy(data[key][sub_key])
                    new_key_detected = True
                else:
                    self.__birth_data[key][sub_key] = copy.deepcopy(data[key][sub_key])
 
        return new_key_detected
    
    def __reset_stream(self):
        print("Resetting the stream with updated birth data...")
        # 1. reconnect existing clients
        # `````````````````````````````````````````````````````````````````````````
        self.__client.disconnect()
        self.__client.connect(self.__data_adp.component_ids)

        # 2. initialize the key-value metadata and respective topic family client
        # `````````````````````````````````````````````````````````````````````````
        trial_id = str(self.__data_adp.cfg.get('trial_id', None))
        kv_topic_family = globals()[TOPIC_CLIENTS['kv'][1]]()
        blob_topic_family = globals()[TOPIC_CLIENTS['blob'][1]]()
        kv_client = globals()[TOPIC_CLIENTS['kv'][0]](copy.deepcopy(self.cfg), kv_topic_family)
        blob_client = globals()[TOPIC_CLIENTS['blob'][0]](copy.deepcopy(self.cfg), blob_topic_family)

        kv_payload = self.__generate_birth_kv_payload()
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
        self.__client.publish_birth(self.__data_adp.attributes, self.__birth_data)
        self.__data_adp.clear_data_buffer()