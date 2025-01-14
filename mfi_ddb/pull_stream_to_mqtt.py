#!/usr/bin/env python3

import time

from mfi_ddb import BaseDataObject, PushStreamToMqtt


class PullStreamToMqtt:

    def __init__(self, cfg_file, data_obj: BaseDataObject) -> None:      
        
        self.data_obj = data_obj
        self.data_obj.get_data()
        
        self.push_stream = PushStreamToMqtt(cfg_file, data_obj)
        self.topics = self.push_stream.topics
        self.cfg = self.push_stream.cfg
        
        if 'stream_rate' not in self.data_obj.cfg:
            print("Stream rate not specified. Defaulting to 1 Hz.")
            self.cfg.stream_rate = 1        
        else:
            self.cfg.stream_rate = self.data_obj.cfg['stream_rate']
            
        try:
            while True:
                self.streamdata()
        except KeyboardInterrupt:
            print("Application interrupted by user. Exiting ...")
                
    def streamdata(self):  
              
        self.data_obj.update_data()
        self.push_stream.streamdata()
    
        component_count = len(self.topics)
        try:
            sleep_time = 1/(self.cfg.stream_rate * component_count)
        except ZeroDivisionError:
            raise ZeroDivisionError("Zero active components.")
        time.sleep(sleep_time)