#!/usr/bin/env python3

import os
import time

import random
import yaml
from omegaconf import OmegaConf
import xmltodict

from mqtt_spb_wrapper import MqttSpbEntityDevice
from ddb.push_stream_to_mqtt_spb import PushStreamToMqttSpb

class PullStreamToMqttSpb:
    def __init__(self, cfg_file, data_obj) -> None:      
        
        self.data_obj.get_data()
        
        self.stream = PushStreamToMqttSpb(cfg_file, data_obj)
    
        try:
            while True:
                self.streamdata()
        except KeyboardInterrupt:
            print("Application interrupted by user. Exiting ...")
                
    def streamdata(self):  
              
        self.data_obj.update_data()
        self.stream.stream_data()
    
        component_count = len(self.components)
        sleep_time = 1/(self.cfg.stream_rate * component_count)
        time.sleep(sleep_time)
