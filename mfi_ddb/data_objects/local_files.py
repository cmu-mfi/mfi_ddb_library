import platform
import time

import yaml
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from mfi_ddb import BaseDataObject


class LocalFilesDataObject(BaseDataObject, FileSystemEventHandler):        
    def __init__(self, config, create_observers=True) -> None:
        super().__init__()
        
        self.config = config
        
        system_config = config['system']
        self.system_name = system_config['name']
        
        # Initializing BaseDataObject data members
        self.component_ids.append(f'lfs/{self.system_name}')
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
        
    def get_data(self):
        if len(self.buffer_data) == 0 and len(self.data[self.system_name]) == 0:
            # Initialize a start text file with config
            time_now = time.strftime("%Y%m%d-%H%M%S")
            
            for each_dir in self.config['watch_dir']:
                with open(f"{each_dir}/mfi_ddb_start_{time_now}.txt", "w") as file:
                    file.write(yaml.dump(self.config))
                    
            self.data[self.system_name]['name'] = "mfi_ddb_start_{time_now}.txt"
            self.data[self.system_name]['timestamp'] = time_now
            self.data[self.system_name]['file'] = bytearray(yaml.dump(self.config),'utf-8')
            
            return
        
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
        
        time.sleep(self.config["wait_before_read"])
        
        data = {}
        data["name"] = self.get_event_data(event, 'name')
        data["timestamp"] = self.get_event_data(event, 'timestamp')
        data["file"] = self.get_event_data(event, 'file')
        
        if len(self.buffer_data) < self.config["buffer_size"]:
            self.buffer_data.append(data)
        else:
            print(f"WARNING: Buffer full. Discarding file: {event.src_path}")
            print("Consider increasing buffer size or stream_rate in config.")

    def get_event_data(self, event, key):
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