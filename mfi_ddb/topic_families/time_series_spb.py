from base import BaseTopicFamily

class SpbTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()

        self.topic_family_name = "spBv1.0"
        
    def process_data(self, data):
        ...
        
    def process_attr(self, attributes):
        ...