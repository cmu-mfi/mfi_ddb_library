from .base import BaseTopicFamily
import json
import jsonschema
import logging

SCHEMA_FILE = "schema/kv.json"
LOGGER = logging.getLogger(__name__)

class KeyValueTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()
        self.topic_family_name = "kv"
        self.schema_validator = self.get_schema_validator()

    def process_data(self, data):
        data = self.apply_defaults(data, self.schema_validator)
        if not self.schema_validator.is_valid(data):
            LOGGER.error(f"Data does not match schema: {self.schema_validator.iter_errors(data)}")
            raise ValueError("Data does not match the MFI DDB key-value schema")
        
        for key in data.keys():
            data[key] = self.__autotype(data[key])
            
        return json.dumps(data)
    
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
    
    @staticmethod
    def apply_defaults(data, validator = None):
        if validator:
            schema = validator.schema
        else:
            schema = KeyValueTopicFamily.get_schema_validator().schema
        for key, value in schema.get("properties", {}).items():
            if "default" in value:
                data.setdefault(key, value["default"])
        return data        
    
    @staticmethod
    def get_schema_validator():
        with open(SCHEMA_FILE, "r") as f:
            schema = json.load(f)
        return jsonschema.Draft7Validator(schema)
        
    @staticmethod
    def process_message(message):
        try:
            data = json.loads(message)
            schema_validator = KeyValueTopicFamily.get_schema_validator()
            data = KeyValueTopicFamily.apply_defaults(data, schema_validator)
            if not schema_validator.is_valid(data):
                LOGGER.error(f"Message does not match schema: {schema_validator.iter_errors(data)}")
                raise ValueError("Message does not match the MFI DDB key-value schema")
            LOGGER.info(f"Message type {data.get('msg_type', 'unknown')} received")
            return data
        except json.JSONDecodeError as e:
            LOGGER.error(f"Failed to decode message: {e}")
            raise ValueError("Message is not valid JSON")