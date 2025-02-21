import time

from mqtt_spb_wrapper import MqttSpbEntityDevice

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.topic_families.base import BaseTopicFamily


class MqttSpb(BaseTopicFamily):
    def __init__(self, config: dict) -> None:
        super().__init__()

        self.cfg = config
        
        #TODO: type hint below to show that dict keys are string, and dict values are MqttSpbEntityDevice
        self._components: dict = {}
             
    def connect(self, component_ids:list):
        
        if 'mqtt' not in self.cfg.keys():
            #TODO: Create config exceptions in mfi_ddb utils
            raise Exception("\'mqtt\' config required in streamer config file")
        else:
            mqtt_keys = ['group_name', 
                        'node_name', 
                        'broker_address']
            #TODO: fix lambda function below
            if 'False' in [a: a in self.cfg['mqtt'].keys for a in mqtt_keys]:
                raise Exception("Config incomplete for mqtt. Following keys needed:",mqtt_keys)
        
        group_name = self.cfg['mqtt']['group_name']
        edge_node_name = self.cfg['mqtt']['node_name']
        mqtt_host = self.cfg['mqtt']['broker_address']
        
        #TODO: find a python equivalent of a=True?10:20 for below five
        mqtt_port = int(self.cfg['mqtt']['broker_port'])
        mqtt_user = self.cfg['mqtt']['username']
        mqtt_pass = self.cfg['mqtt']['password']
        mqtt_tls_enabled = self.cfg['mqtt']['tls_enabled']
        debug = self.cfg['mqtt']['debug']
        
        for component_id in component_ids:
            spb_component = MqttSpbEntityDevice(group_name,
                                                edge_node_name,
                                                component_id,
                                                debug)

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
            raise Exception("No SPB component connected for xxx")
        
        for component_id in self._components.keys():
            
            # check if both attr and data exist for the component_id
            if component_id not in attributes.keys() and \
               component_id not in data.keys():
                print(f"{component_id} attributes or data not available during birth. Removing the component")
                self._components.pop(component_id)
                continue
            
            # set attributes value
            attributes = attributes[component_id]
            attributes = self.process_attr(attributes)
            if not self.__check_attributes():
                raise Exception(f"{self.topic_family_name} not compatible with MqttSpb")
            for key in attributes.keys():
                self._components[component_id].attributes.set_value(key, attributes[key])
                
            # set data values
            input_values = data[component_id]
            input_values = self.process_data(input_values)
            if not self.__check_data(input_values):
                raise Exception(f"{self.topic_family_name} not compatible with MqttSpb")
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
            input_values = self.process_data(input_values)
            if not bool(input_values):
                print(f"Data not found for component {component_id}")
                continue
            elif not self.__check_data(input_values):
                raise Exception(f"{self.topic_family_name} not compatible with MqttSpb")
                      
            for key in input_values.keys():
                # TODO: Add the fix below to the custom mqtt_spb_wrapper class
                data_item_prefix = 'DATA/'+key
                self._components[component_id].data.set_value(data_item_prefix, input_values[key])            
                
            self._components[component_id].publish_data()
            print(f"Data published for component {component_id}")
    
    def disconnect(self):
        pass
    
    def __check_attributes(self, attributes):
        if type(attributes[key]) not in [int, float, str, list]:
            continue                 
        return True
    
    def __check_data(self, data):
        if type(input_values[key]) not in [int, float, str, list]:
            continue         
        return True