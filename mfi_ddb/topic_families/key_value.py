from base import BaseTopicFamily

class KeyValueTopicFamily(BaseTopicFamily):
    def __init__(self):
        super().__init__()
        self.topic_family_name = "kv"