import os

from mfi_ddb import MTConnectDataObject, PullStreamToMqttSpb

if __name__ == "__main__": 
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    mqtt_cfg = os.path.join(current_dir, 'mqtt.yaml')
    mtconnect_cfg = os.path.join(current_dir, 'mtconnect.yaml')
    
    data_obj = MTConnectDataObject(mtconnect_cfg)
    PullStreamToMqttSpb(mqtt_cfg, data_obj)