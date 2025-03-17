from base import BaseTopicFamily

class HistorianTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()

        self.topic_family_name = "historian"
        
    def process_data(self, data):
        ...
        
    def process_attr(self, attributes):
        ...