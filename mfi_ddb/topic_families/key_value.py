from .base import BaseTopicFamily
import json

class KeyValueTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()
        self.topic_family_name = "kv"
        
    def process_data(self, data):
        for key in data.keys():
            data[key] = self.__autotype(data[key])
            
        return data
    
    def __autotype(self, data):
        if not isinstance(data, dict):
            for cast in (int, float, str):
                try:
                    return cast(data)
                except:
                    continue
        else:
            data = json.dumps(data)
            return data        