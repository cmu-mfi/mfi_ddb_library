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
            
        attributes = self.__extract_key_value(attributes, "")
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
    
    def __extract_key_value(self, data_item, data_item_key):
        if len(data_item_key) > 0 and data_item_key[0] == '/':
            data_item_key = data_item_key[1:]
        if isinstance(data_item, dict):
            extracted_data = {}
            for key in data_item.keys():
                substitute_key = key
                extracted_data.update(self.__extract_key_value(data_item[key], f'{data_item_key}/{substitute_key}'))
            return extracted_data
        elif isinstance(data_item, list):
            extracted_data = {}
            for i, item in enumerate(data_item):
                extracted_data.update(self.__extract_key_value(item, f'{data_item_key}_{i}'))
            return extracted_data
        else:
            return {data_item_key: self.__autotype(data_item)}
    
    def __autotype(self, value):
        for cast in (int, float, eval):
            try:
                if cast is eval:
                    return eval(value.replace("true", "True").replace("false", "False"))
                else:
                    if cast is int and '.' in value:
                        return float(value)
                    return cast(value)
            except:
                continue

        return value            