from base import BaseTopicFamily
from schema import blob_pb2

class BlobTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()

        self.topic_family_name = "blob"
        self.trial_id = None
        
    def process_attr(self, attributes):
        self.trial_id = attributes.get("trial_id", None)
        return self.process_data(attributes)
    
    def process_data(self, data):
        ...