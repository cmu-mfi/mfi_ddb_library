import os
import platform
import time

import numpy as np
import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from mfi_ddb import BaseDataObject


class LocalFilesDataObject(BaseDataObject, FileSystemEventHandler):   
    """     
    LocalFilesDataObject is responsible for monitoring local directories for file creation events
    and managing data associated with these events. It extends BaseDataObject and FileSystemEventHandler
    to provide functionality for handling file system events and managing data objects.
    Attributes:
    -----------
    cfg(dict): Configuration dictionary containing system and directory watch settings.
        Expected keys:
            - system: Dictionary containing system name and experiment class.
            - watch_dir: List of directories to watch for file creation events.
            - buffer_size: Maximum number of data entries to store in the buffer.
            - wait_before_read: Time to wait before reading the file content.
    system_name(str): Name of the system, prefixed with 'lfs/'.
    buffer_data(list): List of data dictionaries that have not been staged for publishing to MQTT yet.
    component_ids(list): List of component IDs initialized from BaseDataObject.
    data(dict): Dictionary to store data associated with the system.
    attributes(dict): Dictionary to store attributes associated with the system.
    
    Methods:
    --------
    __init__(config, create_observers=True): Initializes the LocalFilesDataObject with the given configuration and sets up directory observers.
    get_data(): Retrieves data from the buffer and updates the data dictionary.
    update_data(): Calls get_data() to update the data dictionary.
    on_created(event)
    __get_event_data(event, key)
    """
    def __init__(self, config, create_observers=True) -> None:
        super().__init__()
        
        self.cfg = config
        system_config = config['system']
        self.system_name = system_config['name']
        self.system_name = f'lfs/{self.system_name}'
        
        # Initializing BaseDataObject data members
        self.component_ids.append(self.system_name)
        self.data[self.system_name] = {}
        self.attributes[self.system_name] = system_config
        
        self.buffer_data = []
        # buffer_data is a list of data dict that has not been staged 
        # to publish to MQTT yet. FIFO order.
           
        # create a observers for the directories    
        if create_observers:
            for dir in config['watch_dir']: 
                observer = Observer()
                observer.schedule(self, path=dir, recursive=True)
                observer.start()
                print(f"Watching directory {dir}")
        
        # Initialize a start text file with config
        time_now = time.strftime("%Y%m%d-%H%M%S")
        
        for each_dir in self.cfg['watch_dir']:
            filename = os.path.join(each_dir, f"mfi_ddb_start_{time_now}.txt")
            with open(filename, "w") as file:
                file.write(yaml.dump(self.cfg))      
        
        print("Waiting for LocalFilesDataObject to initialize ...")
        time.sleep(self.cfg["wait_before_read"]*2)
        print("LocalFilesDataObject initialized.")
        
    def get_data(self): 
    
        if len(self.buffer_data) > 0:
            data = self.buffer_data.pop(0)
            self.data[self.system_name] = data
            return
        
        self.data[self.system_name] = {}
                
    def update_data(self):
        self.get_data()
        
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
        data["name"] = self.__get_event_data(event, 'name')
        data["timestamp"] = self.__get_event_data(event, 'timestamp')
        data["system"] = self.cfg["system"]
        data["experiment_class"] = self.cfg["system"]["experiment_class"]
        data["file"] = self.__get_event_data(event, 'file')
        
        if len(self.buffer_data) < self.cfg["buffer_size"]:
            self.buffer_data.append(data)
        else:
            print(f"WARNING: Buffer full. Ignoring file: {event.src_path}")
            print("Consider increasing buffer size or stream_rate in config.")

    def __get_event_data(self, event, key):
        """
        Retrieves specific data from the event based on the provided key.
        Parameters:
        -----------
        event : FileSystemEvent
            The event object representing the file creation event.
        key : str
            The key indicating which data to retrieve from the event.
        Returns:
        --------
        Any
            The data corresponding to the provided key. Returns None if the key is not recognized.
        """
        if key == 'name':
            if platform.system() == 'Windows':
                name = event.src_path.split('\\')[-1]
            else:
                name = event.src_path.split('/')[-1]
            print(f"Name: {name}")
            return name
        elif key == 'timestamp':
            format = "%Y%m%d-%H%M%S"
            return time.strftime(format)
        elif key == 'file':
            with open(event.src_path, 'rb') as file:
                data = file.read()
            return data
        else:
            return None