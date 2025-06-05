import time

from .base import BaseTopicFamily
from .schema import blob_pb2

class BlobTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()

        self.topic_family_name = "blob"
        self.__trial_id = None
        
    def process_attr(self, attributes):
        # Keeping last record of trial_id from processing attributes
        self.__trial_id = attributes.get("trial_id", None)
        if self.__trial_id is None:
            print("WARNING: trial_id is not provided in attributes")
        
        payload = self.process_data(attributes, is_attribute=True)
        
        return {'attributes': payload['data']}
    
    def process_data(self, data, is_attribute=False):
        # Using trial_id from data if available, otherwise using last record of trial_id
        if not bool(data):
            print("WARNING: blob data is empty")
            return {}
            
        self.__trial_id = data.get("trial_id", self.__trial_id)        
        
        payload = blob_pb2.Payload()
        payload.timestamp = int(time.time())
        payload.trial_id = self.__trial_id
        
        metric = blob_pb2.Payload.Metric()
        
        metric.name = "data" if not is_attribute else "attributes"
        metric.metadata.file_name = data.get("file_name", "")
        metric.metadata.file_type = data.get("file_type", "")
        metric.metadata.size = data.get("size", 0)
        metric.timestamp = data.get("timestamp", payload.timestamp)
        metric.bytes_value = data.get("file", b'')

        # Put all other data in Metadata.description
        other_metadata = {}
        for key, value in data.items():
            if key not in ["file_name", "file_type", "size", "timestamp", "file"]:
                other_metadata[key] = value
                
        metric.metadata.description = str(other_metadata)
        
        if metric.bytes_value == b'':
            print(f"WARNING: file is not provided in the blob data. Streaming empty file.")
        
        payload.metrics.append(metric)
        
        return({"data": payload.SerializeToString()})
    
    @staticmethod
    def process_message(message):        
        payload = blob_pb2.Payload()
        payload.ParseFromString(message)
                
        metric = payload.metrics[0]

        payload_type = metric.name
        
        data = {}
        data['file_name'] = metric.metadata.file_name
        data['file_type'] = metric.metadata.file_type
        data['size'] = metric.metadata.size
        data['timestamp'] = metric.timestamp
        data['file'] = metric.bytes_value
        
        trial_id = payload.trial_id
        data['trial_id'] = trial_id
        try:
            data['description'] = eval(metric.metadata.description)
        except:
            data['description'] = metric.metadata.description            
            
        return payload_type, data
        
        
            