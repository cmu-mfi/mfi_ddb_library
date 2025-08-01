import time
from ._mqtt import Mqtt


class Subscriber(Mqtt):
    def __init__(self, mqtt_config):
        super().__init__(mqtt_config)
        
        self.connect()
    
    def create_message_callback(self, topic, callback):
        self.client.subscribe(topic)
        
        # pass only the message to callback
        def callback_wrapper(client, userdata, message):
            callback(message.payload)
        
        # set the callback function
        self.client.on_message = callback_wrapper
        print(f"Subscribed to topic: {topic}")
        
    def start_listening(self):
        self.client.loop_forever()
        print("Connected to MQTT broker")
    
        
    