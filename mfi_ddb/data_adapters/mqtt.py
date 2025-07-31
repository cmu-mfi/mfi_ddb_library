import time

import paho.mqtt.client as mqtt

from mfi_ddb.data_adapters.base import BaseDataAdapter
from mfi_ddb.utils.exceptions import ConfigError


class _Mqtt:
    def __init__(self, config: dict) -> None:
        super().__init__()

        self.mqtt_cfg = config        

        mqtt_keys = ['broker_address']
        if 'False' in list(map(lambda a: a in list(self.mqtt_cfg.keys()), mqtt_keys)):
            raise Exception("Config incomplete for mqtt. Following keys needed:",mqtt_keys)        
        
        self.client : mqtt.Client = None
    
    def disconnect(self):
        """
        Disconnect from the MQTT broker and clean up resources.
        """
        if self.client is not None:
            self.client.loop_stop()
            self.client.disconnect()
            print("Disconnected from MQTT broker")
        else:
            print("No MQTT client to disconnect")

    def connect(self):

        mqtt_cfg = self.mqtt_cfg
                
        # REQUIRED KEYS        
        mqtt_host = mqtt_cfg['broker_address']
        
        # OPTIONAL KEYS
        mqtt_port = int(mqtt_cfg['broker_port']) if 'broker_port' in mqtt_cfg.keys() else 1883
        mqtt_user = mqtt_cfg['username'] if 'username' in mqtt_cfg.keys() else None
        mqtt_pass = mqtt_cfg['password'] if 'password' in mqtt_cfg.keys() else None
        if 'password' in mqtt_cfg.keys():
            del self.mqtt_cfg['password']
        mqtt_tls_enabled = mqtt_cfg['tls_enabled'] if 'tls_enabled' in mqtt_cfg.keys() else False
        debug = mqtt_cfg['debug'] if 'debug' in mqtt_cfg.keys() else False
        timeout = mqtt_cfg['timeout'] if 'timeout' in mqtt_cfg.keys() else 5.
        
        self.client = mqtt.Client()
        self.client.username_pw_set(mqtt_user, mqtt_pass)
        self.client.on_connect = self.__on_connect
        self.client.connect(mqtt_host, mqtt_port, 60)
        
        start_time = time.time()
        time_elapsed = 0
        while not self.client.is_connected() or time_elapsed < timeout:
            print(f"Connecting to MQTT broker...{int(time_elapsed)}s")
            time.sleep(1)
            self.client.loop()
            time_elapsed = time.time() - start_time
            
        if not self.client.is_connected():
            raise ConfigError(f"Could not connect to MQTT broker {mqtt_host} after {timeout} seconds.")
    
    def create_message_callback(self, topic, callback):
        self.client.subscribe(topic)
        
        # pass only the message to callback
        def callback_wrapper(client, userdata, message):
            callback(message)
        
        # set the callback function
        self.client.on_message = callback_wrapper
        print(f"Subscribed to topic: {topic}")
        
    def start_listening(self):
        self.client.loop_start()
        print("Connected to MQTT broker")

    def __on_connect(self, client, userdata, flags, rc):
        print(f"Connected to broker {self.mqtt_cfg['broker_address']} with result code {rc}")            


class MqttDataAdapter(BaseDataAdapter, _Mqtt):
    
    CONFIG_HELP = {
        "mqtt": {
            "broker_address": "Address of the MQTT broker",
            "broker_port": "Port of the MQTT broker (default: 1883)",
            "username": "Username for MQTT broker authentication",
            "password": "Password for MQTT broker authentication",
            "tls_enabled": "Enable TLS for MQTT connection (default: False)",
            "debug": "Enable debug mode for MQTT client (default: False)",
            "timeout": "Timeout in seconds for connecting to the MQTT broker (default: 5)"
        },
        
        "trial_id": "Trial ID for the system. No spaces or special characters allowed.",
        "queue_size": "Maximum number of messages to buffer before processing. If the buffer is full, the oldest message will be removed.",
        "topics": "List of topics to subscribe to. Each topic should have a 'component_id' and 'topic' key. Optionally, a 'trial_id' can be provided."
    }
    
    CONFIG_EXAMPLE = {
        "mqtt": {
            "broker_address": "mqtt.example.com",
            "broker_port": 1883,
        },
        "trial_id": "trial_001",
        "queue_size": 10,
        "topics": [
            {
                "component_id": "robot-arm-1",
                "topic": "robot-arm/1/data",
                "trial_id": "trial_001"
            },
            {
                "component_id": "machine-a",
                "topic": "machine/a/data"
            }
        ]
    }
    
    RECOMMENDED_TOPIC_FAMILY = "historian"
    
    def __init__(self, config: dict):
        BaseDataAdapter.__init__(self, config)
        _Mqtt.__init__(self, config['mqtt'])
        
        # CONNECT TO THE MQTT BROKER
        self.connect()
        
        self.buffer_data = {}
        self.queue_size = self.cfg['queue_size'] if 'queue_size' in self.cfg.keys() else 10
        
        # CREATE CALLBACKS FOR THE TOPICS
        for topic in self.cfg['topics']:
            component_id = topic['component_id']
            topic_name = topic['topic']
            if 'trial_id' not in topic.keys():
                topic['trial_id'] = self.cfg['trial_id']
                        
            self.buffer_data[component_id] = []
            self._data[component_id] = {}
            self.attributes[component_id] = topic
            self.component_ids.append(component_id)
            
            self.create_message_callback(topic_name,
                lambda msg, c_id=component_id: self._topic_callback(msg, c_id))
            print(f"Subscribed to topic: {topic}")
        
        self.start_listening()
        print("MqttDataAdapter initialized")
    
    def disconnect(self):
        _Mqtt.disconnect(self)
        return super().disconnect()
    
    def get_data(self):
        for component_id in self.component_ids:
            if len(self.buffer_data[component_id]) > 0:
                data = self.buffer_data[component_id].pop(0)
                self._data[component_id] = data
    
    def _topic_callback(self, message, component_id):
        """
        Callback function to handle incoming MQTT messages.
        """

        topic = message.topic
        payload = message.payload.decode('utf-8')
        payload = self.__autotype(payload)
        print(f"Received message on topic {topic}: {payload}")
        
        # extract key to store the message in the data dictionary
        data = {}
        subscription_topic = self.attributes[component_id]['topic']
        if subscription_topic.split('/')[-1] == '#':
            # If the topic ends with '#', it means we are subscribing to all subtopics
            # We need to extract the subtopic from the received topic
            subtopic = topic[len(subscription_topic):]
            if subtopic.startswith('/'):
                subtopic = subtopic[1:]
        else:
            subtopic = 'data'
        
        if not isinstance(payload, dict):
            data[subtopic] = payload
        else:
            data = self.__extract_key_value(payload, subtopic)
        if len(self.buffer_data[component_id]) >= self.queue_size:
            self.buffer_data[component_id].pop(0)
        
        self.buffer_data[component_id].append(data)
        self._notify_observers({component_id: data})
        
    def __autotype(self, value):
        for cast in (int, float, eval):
            try:
                if cast is eval:
                    return eval(value.replace("true", "True").replace("false", "False"))
                else:
                    return cast(value)
            except:
                continue

        return value
    
    def __extract_key_value(self, data_item, data_item_key):
        if isinstance(data_item, dict):
            for key in data_item.keys():
                substitute_key = key
                return self.__extract_key_value(data_item[key], f'{data_item_key}/{substitute_key}')
        elif isinstance(data_item, list):
            for i, item in enumerate(data_item):
                return self.__extract_key_value(item, f'{data_item_key}_{i}')
        else:
            return {data_item_key: self.__autotype(data_item)}