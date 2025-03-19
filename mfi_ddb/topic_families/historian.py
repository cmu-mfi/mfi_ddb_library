from .base import BaseTopicFamily

MAX_ARRAY_SIZE = 16
class HistorianTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()

        self.topic_family_name = "historian"
        
    def process_attr(self, attributes):
        if 'trial_id' not in attributes.keys():
            print("WARNING: trial_id is not provided in attributes")
            attributes['trial_id'] = None
        
        return self.process_data(attributes)        
        
    def process_data(self, data):
        data_types = [type(data[key]) for key in data.keys()]
        if all([data_type in [int, float, str] for data_type in data_types]):
            return data        
        
        list_idx = [i for i,x in enumerate(data_types) if isinstance(x, list)]
        if len(list_idx) > 0:
            for idx in list_idx:
                if len(data[list(data.keys())[idx]]) > MAX_ARRAY_SIZE:
                    print(f"WARNING: Array size exceeds the maximum limit of {MAX_ARRAY_SIZE}")
                    data.pop(list(data.keys())[idx])
        
        dict_idx = [i for i,x in enumerate(data_types) if isinstance(x, dict)]
        if len(dict_idx) > 0:
            for idx in dict_idx:
                print(f"WARNING: Dictionary is not supported in historian topic family")
                data.pop(list(data.keys())[idx])
                
        return data