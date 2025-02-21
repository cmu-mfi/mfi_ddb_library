import time

import paho.mqtt.client as mqtt

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.topic_families.base import BaseTopicFamily


class MqttJson(BaseTopicFamily):
    def __init__(self, config: dict) -> None:
        super().__init__()

        self.cfg = config
        #TODO: check if correct type hinting
        self.client:mqtt.Client = None
        self._components: list = []
        
        self.__topic_header = self.__get_topic_header(config['mqtt'])
        
    
    def __get_topic_header(config:dict):
        ver = 'mfi_ddb/v1'
        topic_family = self.topic_family_name
        
        #TODO: add a=True?10:20 like logic to assign "" if key doesn't exist
        enterprise = config['enterprise']
        site = config['site']
        area = config['area']
        
        #TODO: merge using function like os.path.join to avoid "//" in case empty string
        return f"{ver}/{topic_family}/{enterprise}/{site}/{area}/"

    def connect(self, component_ids:list):
        
        if 'mqtt' not in self.cfg.keys():
            #TODO: Create config exceptions in mfi_ddb utils
            raise Exception("\'mqtt\' config required in streamer config file")
        else:
            mqtt_keys = ['enterprise', 
                        'broker_address']
            #TODO: fix lambda function below
            if 'False' in [a: a in self.cfg['mqtt'].keys for a in mqtt_keys]:
                raise Exception("Config incomplete for mqtt. Following keys needed:",mqtt_keys)
        
        mqtt_host = self.cfg['mqtt']['broker_address']
        
        #TODO: find a python equivalent of a=True?10:20 for below five
        mqtt_port = int(self.cfg['mqtt']['broker_port'])
        mqtt_user = self.cfg['mqtt']['username']
        mqtt_pass = self.cfg['mqtt']['password']
        mqtt_tls_enabled = self.cfg['mqtt']['tls_enabled']
        debug = self.cfg['mqtt']['debug']
        
        self.client = mqtt.Client()
        self.client.username_pw_set(mqtt_user, mqtt_pass)
        self.client.on_connect = self.__on_connect
        self.client.connect(mqtt_host, mqtt_port, 60)
        
        while not self.client.is_connected():
            print("Connecting to MQTT broker...")
            time.sleep(1)
            self.client.loop()

        self._components = component_ids
        
    def __on_connect(self, client, userdata, flags, rc):
        print(f"All JSON components connected to broker with result code {rc}")            
        
    def publish_birth(self, attributes, data):
        if not bool(self._components):
            #TODO: get class name str and use for error messages
            raise Exception("No JSON component connected for xxx")
        
        self.stream_data(attributes)
        self.stream_data(data)
            
        print(f"Birth published for device {component_id}")
   
    def stream_data(self, data):
        for component_id in data.keys():   
            # check if component_id exist in initialized components
            if component_id not in self._components.keys():
                print(f"{component_id} not initialized for MqttJson. Hence skipping...")
                continue
            
            input_values = data[component_id]
            input_values = self.process_data(input_values)
            if not bool(input_values):
                print(f"Data not found for device {component_id}")
                continue
            elif not self.__check_data(input_values):
                raise Exception(f"{self.topic_family_name} not compatible with MqttJson")
                      
            self.__publish(component_id, input_values)       
                
            self._components[component_id].publish_data()
            print(f"Data published for device {component_id}")
    
    def disconnect(self):
        pass
    
    def __check_data(self, data):       
        return True
    
    def __publish(self, device, payload):
        self.client.publish(topic, payload)
        