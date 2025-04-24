
class BaseTopicFamily:
    def __init__(self):
        self.topic_family_name = "base"
        
    def process_attr(self, attributes):
        return self.process_data(attributes)
    
    def process_data(self, data):
        return data
        
    