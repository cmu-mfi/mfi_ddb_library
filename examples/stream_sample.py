import os

from mfi_ddb import SampleDataObject, PullStreamToMqtt

if __name__ == "__main__": 
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    mqtt_cfg = os.path.join(current_dir, 'mqtt.yaml')
    
    data_obj = SampleDataObject()
    PullStreamToMqtt(mqtt_cfg, data_obj)