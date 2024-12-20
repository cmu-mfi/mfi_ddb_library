#!/usr/bin/env python3

import os
import random
import time

import xmltodict
import yaml
from mqtt_spb_wrapper import MqttSpbEntityDevice
from omegaconf import OmegaConf

from mfi_ddb import BaseDataObject, PushStreamToMqttSpb


class PullStreamToMqttSpb:
    """
    A class to pull data from a data source and stream it to an MQTT broker.
    
    Attributes:
    -----------
    data_obj (BaseDataObject): An object that provides methods to get and update data.
    push_stream (PushStreamToMqttSpb): An instance of the PushStreamToMqttSpb class to handle streaming data to MQTT.
    components (list): A list of components to be streamed.
    cfg (OmegaConf): Configuration object for the streaming process.
    
    Methods:
    --------
    __init__(cfg_file, data_obj): Initializes the PullStreamToMqttSpb class with a configuration file and a data object.
    streamdata(): Updates the data and streams it to the MQTT broker.
    """
    def __init__(self, cfg_file, data_obj: BaseDataObject) -> None:      
        
        self.data_obj = data_obj
        self.data_obj.get_data()
        
        self.push_stream = PushStreamToMqttSpb(cfg_file, data_obj)
        self.components = self.push_stream.components
        self.cfg = self.push_stream.cfg
    
        try:
            while True:
                self.streamdata()
        except KeyboardInterrupt:
            print("Application interrupted by user. Exiting ...")
                
    def streamdata(self):  
              
        self.data_obj.update_data()
        self.push_stream.streamdata()
    
        component_count = len(self.components)
        sleep_time = 1/(self.cfg.stream_rate * component_count)
        time.sleep(sleep_time)