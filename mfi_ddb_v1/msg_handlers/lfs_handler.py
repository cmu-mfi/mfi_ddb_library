import base64
import json
import os
import pickle
import time

import numpy as np
import paho.mqtt.client as mqtt


def lfs_handler(data: dict, output_path):

    # Get current time
    time_val = time.strftime("%Y-%m-%d_%H-%M")
    timestamp = f"{time_val}"
    
    # Check msg type, attributes or data
    if len(data) != 1:
        print(f'Uknown keys available in the message: {data.keys()}')
    else:
        msg_type = list(data.keys())[0]
        data = data[msg_type]
        if msg_type == 'data':
            # check if it has the required keys
            required_keys = ['file', 'name']
            if not all(key in data for key in required_keys):
                print(f"Missing keys in the message: {required_keys}")
                return
        elif msg_type == 'attributes':
            filename = f'{timestamp}.json'
            with open(os.path.join(output_path, filename), 'w') as file:
                file.write(json.dumps(data, indent=4))
            return
        else:
            print(f'Uknown message type: {msg_type}')            
            return

    filename = "untitled"
    ext = ""
    experiment_class = ''.join(np.random.choice(list('0123456789abcdefghijklmnopqrstuvwxyz'), 4))
    
    # Appending the filename with the name, experiment_class and timestamp
    if 'name' in data:
        name = data['name']
        filename = name.split('.')[0]
        ext = name.split('.')[1]
            
    if 'experiment_class' in data:
        if data['experiment_class'] != '':
            experiment_class = data['experiment_class']
    filename += f"_{experiment_class}"
    
    if 'timestamp' in data:
        timestamp = data['timestamp']
    filename += f"_{timestamp}"

    filename += f".{ext}"        
    with open(os.path.join(output_path, filename), 'wb') as file:
        file.write(data['file'])
    
    # Saving metadata as a json file
    metadata = data
    metadata.pop('file')
    filename = filename.split('.')[0]
    with open(os.path.join(output_path, f".{filename}_meta.json"), 'w') as file:
        file.write(json.dumps(metadata, indent=4))
    
    print(f"File {filename} saved to {output_path}")  