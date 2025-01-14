
import os

import yaml

from mfi_ddb import LocalFilesDataObject, PullStreamToMqtt

if __name__ == "__main__": 
    
    current_dir = os.path.dirname(os.path.realpath(__file__))
    mqtt_cfg = os.path.join(current_dir, 'mqtt.yaml')
    localfiles_cfg_file = os.path.join(current_dir, 'localfiles.yaml')
    
    with open(localfiles_cfg_file, 'r') as file:
        localfiles_cfg = yaml.load(file, Loader=yaml.FullLoader)
    
    data_obj = LocalFilesDataObject(localfiles_cfg)
    PullStreamToMqtt(mqtt_cfg, data_obj)