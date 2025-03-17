import time

import paho.mqtt.client as paho_mqtt
from observer import Observer

from mfi_ddb.data_adapters import *
from mfi_ddb.topic_families import *
from mfi_ddb.utils.exceptions import ConfigError

from ._mqtt import Mqtt
from ._mqtt_spb import MqttSpb

TOPIC_CLIENTS = {
    "historian": ("MqttSpb", "HistorianTopicFamily"),
    "kv": ("Mqtt", "KeyValueTopicFamily"),
    "blob": ("Mqtt", "BlobTopicFamily"),
}

class Streamer(Observer):
    def __init__(self, config: dict, data_adp: BaseDataAdapter) -> None:
        super().__init__()
        
        self.cfg = config
        topic_family_name = config['topic_family']
        
        topic_family = globals()[TOPIC_CLIENTS[topic_family_name][1]]
        self.__client = globals()[TOPIC_CLIENTS[topic_family_name][0]](config, topic_family)
        
        self.__data_adp = data_adp
        
        self.__client.connect()
        
        self.__data_adp.get_data()
        self.__client.publish_birth()
        
        self.__last_poll_update = 0
        
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