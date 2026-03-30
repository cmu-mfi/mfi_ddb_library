import requests
from requests.auth import HTTPBasicAuth
import yaml
import logging
from datetime import timezone
from dateutil.parser import parse as parse_date
import error_codes

class PIWebAPI:
    def __init__(self, secrets: dict):        
        self.url = secrets['url']
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(secrets['username'], secrets['password'])
        self.logger = logging.getLogger(__name__)
        self.__secrets = secrets
    
    def get_data_point(self, topic, user_id, timestamp):
        webid = self.__get_topic_webid(topic)        
        iso_time = parse_date(timestamp).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        
        # TODO: Add a condition here to check the user_id attribute of the tag in the ATTR
        # Use "elements" query to get webid for the ATTR
        # Search for the user_id in the returned attributes and if it matches, then proceed with the request. 
        # If not, return an error or None.
        
        get_request = f"{self.url}/streams/{webid}/recordedattime"
        parameters = {
            "time": iso_time,
            "selectedFields": "Value"
        }
        get_request += "?" + "&".join([f"{key}={value}" for key, value in parameters.items()])
        response = self.__make_get_request(get_request)
        return response.get('Value', None)
    
    def get_data_range(self, topic, user_id, start_time, end_time, page_size, page_token):
        
        #1. PARSE THE INPUTS
        webid = self.__get_topic_webid(topic)
        # iso_start_time = parse_date(start_time).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # iso_end_time = parse_date(end_time).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        # iso_page_token = parse_date(page_token).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ") if page_token else None
        iso_start_time = parse_date(start_time).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        iso_end_time = parse_date(end_time).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        #Due to python truncating of milliseconds, this was updated to keep original values
        iso_page_token = page_token if page_token else None
        
        if iso_page_token:
            iso_start_time = iso_page_token

        if iso_start_time >= iso_end_time:
            raise error_codes.InvalidArgumentError("start_time must be less than end_time")
        
        # TODO: Add a condition here to check the user_id attribute of the tag in the ATTR
        # Use "elements" query to get webid for the ATTR
        # If multiple values exist, get the time
        
        # 2. CHECK THE DATA POINT COUNT FOR THE GIVEN RANGE
        get_request = f"{self.url}/streams/{webid}/summary"

        parameters = {
            "startTime": iso_start_time,
            "endTime": iso_end_time,
            "summaryType": "Count",
        }

        get_request += "?" + "&".join([f"{key}={value}" for key, value in parameters.items()])
        response = self.__make_get_request(get_request)

        total_count = response['Items'][0]['Value']['Value'] if response['Items'] else 0
        request_count = int(min(page_size, total_count))

        if request_count == 0:
            return {
                "data": [],
                "nextPageToken": None
            }
        
        # 3. GET THE DATA POINTS FOR THE GIVEN RANGE
        get_request = f"{self.url}/streams/{webid}/recorded"
        parameters.pop("summaryType", None)
        parameters["maxCount"] = str(request_count)
        parameters["selectedFields"] = "Items.Timestamp;Items.Value"
        get_request += "?" + "&".join([f"{key}={value}" for key, value in parameters.items()])
        response = self.__make_get_request(get_request)
        items = response.get('Items', [])

        #checking if the item was already sent in the previous page comparing timestamps, to avoid duplicate data sent
        if iso_page_token and items and items[0]['Timestamp'] == iso_page_token:
            items = items[1:]

        last_timestamp = items[-1]['Timestamp'] if items else None
        data = []
        for item in items:
            data.append(item["Value"])
        
        return {
            "data": data,
            "nextPageToken": last_timestamp if items and last_timestamp != page_token else None
        }
        
        
    def stream_data(self, topic, user_id, start_from):
        raise NotImplementedError("Streaming not implemented yet")
    
    def __get_topic_webid(self, topic):
        get_request = f"{self.url}/points"
        
        parameters = {
            "path": f"\\\\{self.__secrets['dataserver']['name']}\\{self.__secrets['mqtt_connector']['path']}/{topic}",
            "selectedFields": "WebId"
        }
        get_request += "?" + "&".join([f"{key}={value}" for key, value in parameters.items()])

        # example:
        # https://fms-vision.fms.local.cmu.edu/piwebapi/points?path=\\pgh-acdpi-01\MQTT_Connector.pgh-mfimqtt-01/mfi-v1.0-historian/Mill-19-test/Mezzanine-Lab/yk_destroyer/DATA/tool_offset/data/data.0
        response = self.__make_get_request(get_request)
        webid = response.get('WebId', None)
        if webid is None:
            self.logger.error(f"WebId not found for topic: {topic}")
            raise error_codes.NotFoundError(f"WebId not found for topic: {topic}")
        
        return webid       
        
    def __make_get_request(self, url):
        
        response = self.session.get(url)
        
        if response.status_code != 200:
            self.logger.error(f"GET request failed. URL: {url}, Status Code: {response.status_code}, Response: {response.text}")
            raise error_codes.InternalError(f"GET request failed. Status Code: {response.status_code}")
        return response.json()