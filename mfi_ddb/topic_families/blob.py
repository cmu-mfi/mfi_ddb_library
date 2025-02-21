from base import BaseTopicFamily

class BlobTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()

        self.topic_family_name = "blob"
        
    def process_data(self, data):
        ...