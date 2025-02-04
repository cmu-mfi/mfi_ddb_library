#!/usr/bin/env python3

import threading
import time

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
        
        if 'stream_rate' not in self.data_obj.cfg:
            print("Stream rate not specified. Defaulting to 1 Hz.")
            self.cfg.stream_rate = 1        
        else:
            self.cfg.stream_rate = self.data_obj.cfg['stream_rate']
        
        self.__last_update_time = None    
        
        try:
            while True:
                self.streamdata()
        except KeyboardInterrupt:
            print("Application interrupted by user. Exiting ...")
                
    def streamdata(self):  
              
        self.data_obj.update_data()
        
        stream_thread = threading.Thread(target=self.push_stream.streamdata)
        stream_thread.start()
            
        component_count = len(self.components)
        try:
            sleep_time = 1/(self.cfg.stream_rate * component_count) # seconds
        except ZeroDivisionError:
            raise ZeroDivisionError("Zero active components.")
        
        if self.__last_update_time is not None:
            time_diff = time.time() - self.__last_update_time
            sleep_time -= time_diff
            if sleep_time < 0:
                print(f"WARNING: Stream rate is too high. Data update and stream took longer ({time_diff:.3f}s vs {sleep_time+time_diff:.3f}s).")
                sleep_time = 0
                
        print(f"Polling next after {sleep_time:.3f} seconds.")
        time.sleep(sleep_time)
        self.__last_update_time = time.time()
        