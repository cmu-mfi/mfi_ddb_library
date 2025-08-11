import base64
import os
import platform
import socket
import time
from typing import List

import numpy as np
import yaml
from pydantic import BaseModel, Field
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from mfi_ddb.data_adapters.base import BaseDataAdapter


class LocalFilesDataAdapter(BaseDataAdapter, FileSystemEventHandler):
    
    NAME = "Local Files"
    
    CONFIG_HELP = {
        "watch_dir": "List of directories to watch for new files. The first directory will be used to create a starter file.",
        "buffer_size": "Maximum number of files to buffer before streaming. If the buffer is full, the oldest file will be removed.",
        "wait_before_read": "Time in seconds to wait before reading a new file after it is created. This is to ensure the file is fully written.",
        "system": {
            "name": "Name of the system",
            "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
            "description": "Description of the system",
            "other_attributes": "Other attributes of the system"
        }
    }
    
    CONFIG_EXAMPLE = {
        "watch_dir": ["/path/to/watch/dir"],
        "buffer_size": 10,
        "wait_before_read": 5,
        "system": {
            "name": "local_files_system",
            "trial_id": "trial_001",
            "description": "Local files data adapter system",
            "manufacturer": "Example Corp",
            "model": "LocalFilesModel"
        }
    }
    
    RECOMMENDED_TOPIC_FAMILY = "blob"
    
    CALLBACKS_SUPPORTED = True
    
    class _SystemInfo(BaseModel):
        trial_id: str = Field(..., description="Trial ID for the system. No spaces or special characters allowed.")
        name: str = Field(..., description="Name of the system.")
                
        model_config = {
            "extra": "allow"
        }        
    
    class SCHEMA(BaseDataAdapter.SCHEMA):
        watch_dir: List[str] = Field(..., description="List of directories to watch for new files.")
        buffer_size: int = Field(..., description="Maximum number of files to buffer before streaming.")
        wait_before_read: int = Field(..., description="Time in seconds to wait before reading a new file.")
        system: "_SystemInfo" = Field(..., description="System information including trial ID, name, and other attributes.")

    def __init__(self, config: dict = None) -> None:
        super().__init__(config)
        
        system_config = config['system']
        self.system_name = system_config['name']
        
        self.component_ids.append(self.system_name)
        self._data[self.system_name] = {}
        self.attributes[self.system_name] = system_config
        
        self.buffer_data = []
        # buffer_data is a list of data dict that has not been staged 
        # to publish to MQTT yet. LIFO order.
           
        # create a observers for the directories    
        for dir in config['watch_dir']: 
            observer = Observer()
            observer.schedule(self, path=dir, recursive=True)
            observer.start()
            print(f"Watching directory {dir}")    
        
        print("Waiting for LocalFilesDataAdapter to initialize ...")
        time.sleep(self.cfg["wait_before_read"]*2)
        print("LocalFilesDataAdapter initialized.")
        
        self.__create_starter_file()        
        
    def get_data(self): 
        if len(self.buffer_data) > 0:
            data = self.buffer_data.pop(0)
            self._data[self.system_name] = data
            return        
                    
    def on_created(self, event):
        """
        Handles the event when a new file is created in the watched directory.
        Parameters:
        -----------
        event : FileSystemEvent
            The event object representing the file creation event.
        """
        if event.is_directory:
            return
        
        print(f"New file created: {event.src_path}")
        
        time.sleep(self.cfg["wait_before_read"])
        
        data = {}
        data["file_name"] = self.__get_event_data(event, 'file_name')
        data["file_type"] = self.__get_event_data(event, 'file_type')
        data["file_path"] = self.__get_event_data(event, 'file_path')       
        data["timestamp"] = self.__get_event_data(event, 'timestamp')
        data["file"] = self.__get_event_data(event, 'file')
        data["size"] = self.__get_event_data(event, 'size')
        
        data["trial_id"] = self.cfg["system"]["trial_id"]
        data["system"] = self.cfg["system"]
        
        if len(self.buffer_data) >= self.cfg["buffer_size"]:
            print(f"WARNING: Buffer full. Ignoring file {self.buffer_data[-1]['file_name']}")
            print("Consider increasing buffer size or streaming_rate.")
            self.buffer_data.pop(0)
            
        self.buffer_data.append(data)
        self._notify_observers({self.system_name: data})

    def update_config(self, config: dict):
        """
        Update the configuration of the data object with the new configuration.
        
        Args:
            config (dict): The new configuration of the data object.
        """
        if bool(config):
            self.cfg = config
            self.__create_starter_file()
        else:
            raise ValueError("The configuration is empty!")

    def __get_event_data(self, event, key):
        if key == 'file_name':
            if platform.system() == 'Windows':
                name = event.src_path.split('\\')[-1]
            else:
                name = event.src_path.split('/')[-1]
            print(f"Name: {name}")
            return name
        elif key == 'file_type':
            return os.path.splitext(event.src_path)[1]
        elif key == 'file_path':
            return event.src_path
        elif key == 'timestamp':
            return int(time.time())
        elif key == 'file':
            file_data = None
            with open(event.src_path, 'rb') as file:
                file_data = file.read()
            return file_data
        elif key == 'size':
            return os.path.getsize(event.src_path)
        else:
            return None    

    def __create_starter_file(self):
        target_dir = self.cfg['watch_dir'][0]
        time_now = time.strftime("%Y%m%d-%H%M%S")
        filename = os.path.join(target_dir, f"mfi_ddb_start_{time_now}.txt")
        
        source_info = {}
        source_info['hostname'] = socket.gethostname()
        source_info['os'] = platform.system()
        source_info['fqdn'] = socket.getfqdn()
        
        file_dict = {}
        file_dict['source_info'] = source_info
        file_dict['config'] = self.cfg
        
        with open(filename, "w") as file:
            file.write(yaml.dump(file_dict))