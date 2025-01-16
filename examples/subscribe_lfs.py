import os

import yaml
from mfi_ddb import MqttSubscriber

if __name__ == '__main__':
    
    # LOAD CONFIG FILES
    current_dir = os.path.dirname(os.path.abspath(__file__))
    config_dir = os.path.join(current_dir, 'config')
    
    mqtt_config = os.path.join(config_dir, 'mqtt.yaml')
    mqtt_cfs_config = os.path.join(config_dir, 'config.yaml')
    
    with open(mqtt_config, 'r') as file:
        secret = yaml.safe_load(file)
        
    with open(mqtt_cfs_config, 'r') as file:
        config = yaml.safe_load(file)


    # MQTT SUBSCRIBER
    mqtt_pub = MqttSubscriber(secret, config)
    
    mqtt_pub.client.loop_forever()