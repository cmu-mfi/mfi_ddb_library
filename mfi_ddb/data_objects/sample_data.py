import time

import numpy as np
import yaml

from mfi_ddb import BaseDataObject


class SampleDataObject(BaseDataObject):   
    def __init__(self, config={}) -> None:
        super().__init__()
        self.cfg = config
        self.component_ids.append("sample-component")
        self.attributes["sample-component"] = {"experiment_class": "testing-mqtt"}
        self.data["sample-component"] = {"var_1": 0}
        
    def get_data(self): 
        self.data["sample-component"]["var_1"] = np.random.rand()
                
    def update_data(self):
        self.get_data()