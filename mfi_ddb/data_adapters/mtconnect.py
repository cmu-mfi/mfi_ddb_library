import time

import omegaconf
import requests
import xmltodict
from omegaconf import OmegaConf
from ping3 import ping

from mfi_ddb.data_adapters.base import BaseDataAdapter


class MTconnectDataAdapter(BaseDataAdapter):
    def __init__(self, config: dict):
        super().__init__()
        
        self.cfg = config
        self.device_name = self.cfg['mtconnect']['device_name']
        # CHECK IF MTCONNECT AGENT IS ACTIVE
        self.__connect()
        
        # POPULATE COMPONENT IDS AND ATTRIBUTES
        # Note: By default, only first device, from the list of MTConnect devices, is used.
        #       If multiple devices exist, specify config['mtconnect']['mtconnect_uuid'] to use a specific device.
        probe_data_raw = self.__request_agent('probe')
        device_data = probe_data_raw.MTConnectDevices.Devices.Device
        if isinstance(device_data, omegaconf.listconfig.ListConfig):
            if 'mtconnect_uuid' in self.cfg['mtconnect'].keys():
                device_id = self.cfg['mtconnect']['mtconnect_uuid']
                for device in device_data:
                    if device['@uuid'] == device_id:
                        device_data = device
                        break
            else:
                device_data = device_data[0]
                
        current_time = time.time()
            
        # Populate device attributes
        device_attributes = {}
        device_attributes['trial_id'] = self.cfg['mtconnect']['trial_id']
        for key in device_data.keys():
            if not isinstance(device_data[key], omegaconf.dictconfig.DictConfig):
                device_attributes[key] = device_data[key]
            elif key == 'Description':
                for sub_key in device_data[key].keys():
                    if not isinstance(device_data[key][sub_key], omegaconf.dictconfig.DictConfig):
                        device_attributes[sub_key] = device_data[key][sub_key]

        # Populate component and data attributes
        components_data = device_data
        component_data = self.__get_probe_components(components_data)
        for component in component_data:
            component_id = f'{self.device_name}/{component["@id"]}'
            self.last_updated[component_id] = current_time
            self.component_ids.append(component_id)
            self.attributes[component_id] = device_attributes.copy()
            self._data[component_id] = {}
            data_list = component.DataItems.DataItem
            if not isinstance(data_list, omegaconf.listconfig.ListConfig):
                data_list = [data_list]
            for data_item in data_list:
                for key in data_item.keys():
                    self._data[component_id][f'{data_item["@id"]}/{key}'] = str(data_item[key])                
                    
    def get_data(self):
        raw_data = self.__request_agent('current')

        component_stream = raw_data.MTConnectStreams.Streams.DeviceStream.ComponentStream
        if not isinstance(component_stream, omegaconf.listconfig.ListConfig):
            component_stream = [component_stream]
            
        self.__populate_data(component_stream)
    
    def update_data(self):
        raw_data = self.__request_agent('sample')
        
        component_stream = raw_data.MTConnectStreams.Streams.DeviceStream.ComponentStream
        if not isinstance(component_stream, omegaconf.listconfig.ListConfig):
            component_stream = [component_stream]
        
        self.__populate_data(component_stream)
        
        
    def __connect(self):
        # Ping to see if MTConnect agent is active -----------------------------------
        print("Checking if MTConnect agent is active ...")
        ip = self.cfg['mtconnect']['agent_ip']
        
        response = ping(ip)
        
        while response is None:
            print("MTConnect agent is not active. Waiting ...")
            time.sleep(1)
            response = ping(ip)
        
        print(f"MTConnect agent at {ip} is active")    
        
    def __request_agent(self, ext: str):
        URL = str(self.cfg['mtconnect']['agent_url']).rstrip("/")
        response = requests.get(URL + ext)
        val = xmltodict.parse(response.text, encoding='utf-8')
        val = OmegaConf.create(val)        
        return val
            
    def __get_probe_components(self, data, component_data=None):
        if component_data is None:
            component_data = []
        
        if isinstance(data, omegaconf.dictconfig.DictConfig):
            if 'DataItems' in data.keys():
                component_data.append(data)
            for key in data.keys():
                self.__get_probe_components(data[key], component_data)
        elif isinstance(data, omegaconf.listconfig.ListConfig):
            for item in data:
                self.__get_probe_components(item, component_data)
        
        return component_data

    def __populate_data(self, raw_data):     #populates the data from xml to dict
        
        current_time = time.time()
        
        for component_data in raw_data:
            component_id = f'{self.device_name}/{component_data["@componentId"]}'
            
            def extract_key_value(data_item, data_item_key):
                if isinstance(data_item, omegaconf.dictconfig.DictConfig):
                    for key in data_item.keys():
                        substitute_key = key
                        if key == '#text':
                            substitute_key = 'value'
                        extract_key_value(data_item[key], f'{data_item_key}/{substitute_key}')
                elif isinstance(data_item, omegaconf.listconfig.ListConfig):
                        for i, item in enumerate(data_item):
                            extract_key_value(item, f'{data_item_key}_{i}')
                else:
                    try:
                        self._data[component_id][data_item_key] = self.__autotype(data_item)           
                    except KeyError:
                        print("WARNING: KeyError: ", data_item_key, " not found in data buffer for component ", component_id)
                    self.last_updated[component_id] = current_time
            
            for key in component_data.keys():
                if key in ['Events', 'Samples']:
                    for data_item_list in component_data[key].values():
                        if not isinstance(data_item_list, omegaconf.listconfig.ListConfig):
                            data_item_list = [data_item_list]                            
                        for data_item in data_item_list:
                            data_item_id = data_item['@dataItemId']
                            extract_key_value(data_item, data_item_id)                                                         

    def __autotype(self, value):
        try:
            return int(value)
        except:
            try:
                return float(value)
            except:
                return value
