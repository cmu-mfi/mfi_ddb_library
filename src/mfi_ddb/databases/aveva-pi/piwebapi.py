import requests
from requests.auth import HTTPBasicAuth
import yaml
import logging

class PIWebAPI:
    def __init__(self, secrets: dict):        
        self.url = secrets['url']
        self.auth = HTTPBasicAuth(secrets['username'], secrets['password'])
        self.logger = logging.getLogger(__name__)
    
    def get_data_point(self, topic, user_id, timestamp):
        url = self.__topic_to_piwebapi_url(topic)
        ...
    
    def get_data_range(self, topic, user_id, start_time, end_time, page_size, page_token):
        url = self.__topic_to_piwebapi_url(topic)
        ...
        
    def stream_data(self, topic, user_id, start_from):
        raise NotImplementedError("Streaming not implemented yet")
    
    def __topic_to_piwebapi_url(self, topic):
        ...