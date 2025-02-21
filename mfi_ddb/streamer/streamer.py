import time

import paho.mqtt.client as mqtt
from observer import Observer

from mfi_ddb.data_adapters.base import BaseDataAdapter


class Streamer(Observer):
    def __init__(self, config: dict, data_adp: BaseDataAdapter) -> None:
        super().__init__()
        
        self.cfg = config
        topic_family_name = config['topic_family']
        # TODO: Based on topic family name choose which class to inherit for converting data to intended payload for streaming
        # TODO: spbv topic family also prefixes metric name keys with site/area if provided in mqtt config
        ...
        
        self.data_adp = data_adp
        
        self.connect()
        
        self.data_adp.get_data()
        self.publish_birth()
        
        # private data members
        self.__last_poll_update = 0
        
    def reconnect(self, config=self.cfg):
        self.disconnect()
        
        wait_time = 1
        print(f"Trying to reinitialize Data Adapter and connect in {wait_time} second...")
        time.sleep(wait_time)
        
        data_adp = self.data_adp.__class__(self.data_adp.cfg)
        self.__init__(config, data_adp)
    
    def poll_and_stream_data(self, polling_rate_hz: int = 1):
        
        while (time.time() - self.__last_poll_update) > 1:
            time.sleep(0.1)            
        
        self.data_adp.get_data()
        self.stream_data(self.data_adp.data)
        self.clear_data_buffer()
                
        self.__last_poll_update = time.time()
        
        
    def on_data_update(self, data: dict):
        self.stream_data(data)
        self.clear_data_buffer(list(data.keys()))