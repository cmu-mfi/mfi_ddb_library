import copy
import time
from typing import Dict

from mfi_ddb.streamer.mqtt_spb_wrapper import MqttSpbEntityDevice

from mfi_ddb.topic_families.base import BaseTopicFamily
from mfi_ddb.utils.exceptions import ConfigError

MAX_ARRAY_SIZE = 16

class MqttSpb:
    def __init__(self, config: dict, topic_family: BaseTopicFamily) -> None:
        super().__init__()

        self.cfg = config

        if 'mqtt' not in self.cfg.keys():
            raise ConfigError("\'mqtt\' config required in streamer config file")
        else:
        
            mqtt_keys = ['enterprise', 
                        'site', 
                        'broker_address']
            if 'False' in list(map(lambda a: a in list(self.cfg['mqtt'].keys()), mqtt_keys)):
                raise ConfigError("Config incomplete for mqtt. Following keys needed:",mqtt_keys)
        
        self._topic_family = topic_family
        self._components: Dict[str, MqttSpbEntityDevice] = {}
             
    def connect(self, component_ids:list):
              
        mqtt_cfg = self.cfg['mqtt']
        
        # REQUIRED KEYS
        group_name = mqtt_cfg['enterprise']
        edge_node_name = mqtt_cfg['site']
        mqtt_host = mqtt_cfg['broker_address']
        
        # OPTIONAL KEYS
        mqtt_port = int(mqtt_cfg['broker_port']) if 'broker_port' in mqtt_cfg.keys() else 1883
        mqtt_user = mqtt_cfg['username'] if 'username' in mqtt_cfg.keys() else None
        mqtt_pass = mqtt_cfg['password'] if 'password' in mqtt_cfg.keys() else None
        if 'password' in mqtt_cfg.keys():
            del self.cfg['mqtt']['password']  # Remove password from config for security reasons
                    
        mqtt_tls_enabled = mqtt_cfg['tls_enabled'] if 'tls_enabled' in mqtt_cfg.keys() else False
        debug = mqtt_cfg['debug'] if 'debug' in mqtt_cfg.keys() else False
        
        for component_id in component_ids:
            spb_component = MqttSpbEntityDevice(group_name,
                                                edge_node_name,
                                                component_id,
                                                debug_enabled = debug)

            _connected = False
            while not _connected:
                _connected = spb_component.connect(mqtt_host,
                                                mqtt_port,
                                                mqtt_user,
                                                mqtt_pass,
                                                mqtt_tls_enabled)
                if not _connected:
                    print("  Error, could not connect. Trying again in a few seconds ...")
                    time.sleep(3)

            self._components[component_id] = spb_component
        print(f"All SPB components connected to broker") 
            
    def publish_birth(self, attributes, data):
        if not bool(self._components):
            #TODO: get class name str and use for error messages
            raise Exception("No SPB component connected")
        
        for component_id in self._components.keys():
            
            # check if both attr and data exist for the component_id
            if component_id not in attributes.keys() and \
               component_id not in data.keys():
                print(f"{component_id} attributes or data not available during birth. Removing the component")
                self._components.pop(component_id)
                continue
            
            # set attributes value
            component_attr = attributes[component_id]
            component_attr = self._topic_family.process_attr(component_attr)
            if not self.__check_attributes(component_attr):
                raise Exception(f"{self._topic_family.topic_family_name} not compatible with MqttSpb")
            for key in component_attr.keys():
                self._components[component_id].attributes.set_value(key, component_attr[key])
                
            # set data values
            input_values = data[component_id]
            input_values = self._topic_family.process_data(input_values)
            if not self.__check_data(input_values):
                raise Exception(f"{self._topic_family.topic_family_name} not compatible with MqttSpb")
            for key in input_values.keys():                
                self._components[component_id].data.set_value(key, input_values[key])
                
            # publish
            self._components[component_id].publish_birth()
            
            print(f"Birth published for component {component_id}")
            
        return True
   
    def stream_data(self, data):
        for component_id in data.keys():   
            # check if component_id exist in initialized components
            if component_id not in self._components.keys():
                print(f"{component_id} not initialized for MqttSpb. Hence skipping...")
                continue
            
            input_values = data[component_id]
            input_values = self._topic_family.process_data(input_values)
            if not bool(input_values):
                print(f"Data not found for component {component_id}")
                continue
            elif not self.__check_data(input_values):
                raise Exception(f"{self._topic_family.topic_family_name} not compatible with MqttSpb")
                      
            for key in input_values.keys():
                # TODO: Add the fix below to the custom mqtt_spb_wrapper class
                data_item_prefix = 'DATA/'+key
                self._components[component_id].data.set_value(data_item_prefix, input_values[key])            
            
            self._components[component_id].publish_data()
            print(f"Data published for component {component_id}")
    
    def disconnect(self):
        pass
    
    def __check_attributes(self, attributes: dict):
        return self.__check_data(attributes)
    
    def __check_data(self, data: dict):
        for key in data.keys():
            if type(data[key]) not in [int, float, str, list]:
                return False         
            if type(data[key]) == list:
                if len(data[key]) > MAX_ARRAY_SIZE:
                    return False
        return True
    
    def set_death_payload(self, topic: str, payload: str, qos: int = 0, retain: bool = False):
        raise NotImplementedError("Death payload is not implemented for MqttSpb")