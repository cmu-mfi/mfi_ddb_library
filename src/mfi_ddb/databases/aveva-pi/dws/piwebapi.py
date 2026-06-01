import os
import sys

from flask import json
import requests
from requests.auth import HTTPBasicAuth
import logging
from datetime import timezone
from dateutil.parser import parse as parse_date
sys.path.insert(0, os.path.dirname(__file__))
from error_codes import GrpcError

class PIWebAPI:
    def __init__(self, secrets: dict):        
        self.url = secrets['url']
        self.session = requests.Session()
        self.session.auth = HTTPBasicAuth(secrets['username'], secrets['password'])
        self.logger = logging.getLogger(__name__)
        self.__secrets = secrets
    
    def get_data_point(self, topic, timestamp, do_closest_past= True):
     
        webids_topic_map = self.__get_topic_webid(topic)

        # If no match or more than one match, do not continue
        if len(webids_topic_map) != 1:
            self.logger.info(
                f"Skipping get_data_point for topic '{topic}' because "
                f"{len(webids_topic_map)} WebIds were found."
            )
            return None

        webid, topic_name = next(iter(webids_topic_map.items()))
        iso_time = parse_date(timestamp).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        retrieval_mode = "AtOrBefore" if do_closest_past else "AtOrAfter"

        get_request = f"{self.url}/streams/{webid}/recordedattime"

        parameters = {
            "time": iso_time,
            "retrievalMode": retrieval_mode,
            "selectedFields": "Timestamp;Value"
        }

        get_request += "?" + "&".join(
            [f"{key}={value}" for key, value in parameters.items()]
        )

        self.logger.debug(f"GET request URL: {get_request}")

        response = self.__make_get_request(get_request)

        self.logger.debug(f"RecordedAtTime response: {response}")

        return {
            "timestamp": response.get("Timestamp"),
            "topic": topic_name,
            "value": response.get("Value"),
        }

    def get_data_range(self, topic, start_time, end_time, page_size, page_token):
        
        webid_topic_map = self.__get_topic_webid(topic)
        self.logger.debug(f"Resolved {len(webid_topic_map)} WebIds for topic '{topic}': {webid_topic_map}")
        
        if not webid_topic_map:
            self.logger.info(f"No WebIds found for topic '{topic}', returning empty result")
            return {"data": [], "nextPageToken": None}
        
        iso_start_time = parse_date(start_time).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        iso_end_time = parse_date(end_time).astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
        self.logger.debug(f"Time range: {iso_start_time} -> {iso_end_time}, page_size={page_size}, page_token={page_token}")
        
        if iso_start_time >= iso_end_time:
            raise GrpcError.InvalidArgumentError("start_time must be less than end_time")
        
        token_map = json.loads(page_token) if page_token else {}
        
        # 1. FETCH EACH STREAM INDEPENDENTLY WITH ITS OWN TOKEN
        all_items = []
        for webid, topic_name in webid_topic_map.items():
            stream_start = token_map.get(webid, iso_start_time)
            self.logger.debug(f"Stream '{topic_name}' | webid={webid} | stream_start={stream_start}")
            
            if stream_start >= iso_end_time:
                self.logger.debug(f"Stream '{topic_name}' is exhausted, skipping")
                continue
            
            get_request = f"{self.url}/streams/{webid}/recorded"
            parameters = {
                "startTime": stream_start,
                "endTime": iso_end_time,
                "maxCount": str(page_size),
                "selectedFields": "Items.Timestamp;Items.Value",
            }
            get_request += "?" + "&".join([f"{key}={value}" for key, value in parameters.items()])
            self.logger.debug(f"GET request: {get_request}")
            
            response = self.__make_get_request(get_request)
            items = response.get('Items', [])
            self.logger.debug(f"Stream '{topic_name}' returned {len(items)} items")
            
            # Strip pagination boundary duplicate
            if token_map.get(webid) and items and items[0]['Timestamp'] == stream_start:
                self.logger.debug(f"Stripping boundary duplicate for stream '{topic_name}'")
                items = items[1:]
            
            for item in items:
                all_items.append({
                    "timestamp": item["Timestamp"],
                    "topic": topic_name,
                    "value": item["Value"],
                    "_webid": webid,
                })
        
        self.logger.debug(f"Total items fetched across all streams: {len(all_items)}")
        
        # 2. SORT AND TRIM TO PAGE SIZE
        all_items.sort(key=lambda x: x['timestamp'])
        page_items = all_items[:page_size]
        self.logger.debug(f"After trim to page_size={page_size}: {len(page_items)} items")
        
        # 3. BUILD NEXT TOKEN MAP based on what was actually returned
        next_token_map = {}
        for item in page_items:
            next_token_map[item['_webid']] = item['timestamp']
        
        # Carry forward tokens for streams with no data this page
        for webid in webid_topic_map:
            if webid not in next_token_map and webid in token_map:
                next_token_map[webid] = token_map[webid]
        
        has_more = len(all_items) > page_size
        self.logger.debug(f"has_more={has_more}, next_token_map={next_token_map}")
        
        # 4. STRIP INTERNAL FIELDS BEFORE RETURNING
        data = [
            {
                "timestamp": item["timestamp"],
                "topic": item["topic"],
                "value": item["value"],
            }
            for item in page_items
        ]
        
        self.logger.debug(f"Returning {len(data)} datapoints")
        return {
            "data": data,
            "nextPageToken": json.dumps(next_token_map) if has_more else None
        }
        
    def stream_data(self, topic, user_id, start_from):
        raise NotImplementedError("Streaming not implemented yet")
    
    def __get_topic_webid(self, topic):
        """
        Always returns a dict of {webid: topic_name}.
        Example:
        {"WEBID1": "sensors/temp"}
        or
        {"WEBID1": "sensors/temp", "WEBID2": "sensors/humidity"}
        """
        self.logger.info(f"Resolving WebIds for topic: {topic}")
        
        # Wildcard search
        if topic.endswith("/#"):
            self.logger.info("Detected wildcard topic lookup")
            topic_stripped = topic[:-2]
            name_filter = (
                f"{self.__secrets['mqtt_connector']['path']}/{topic_stripped}*"
            )
            get_request = (
                f"{self.url}/dataservers/"
                f"{self.__secrets['dataserver']['webid']}/points"
            )
            parameters = {
                "nameFilter": name_filter,
                "selectedFields": "Items.WebId;Items.Name"  # added Name
            }
            get_request += "?" + "&".join(
                [f"{key}={value}" for key, value in parameters.items()]
            )
            response = self.__make_get_request(get_request)
            items = response.get("Items", [])
            
            # Strip the path prefix to get back the topic name
            prefix = f"{self.__secrets['mqtt_connector']['path']}/"
            result = {
                item["WebId"]: item["Name"].replace(prefix, "", 1)
                for item in items
                if "WebId" in item and "Name" in item
            }
            self.logger.info(f"Wildcard lookup found {len(result)} WebIds")
            return result

        # Exact point lookup
        else:
            self.logger.info("Detected exact topic lookup")
            get_request = f"{self.url}/points"
            full_path = (
                f"\\\\{self.__secrets['dataserver']['name']}\\"
                f"{self.__secrets['mqtt_connector']['path']}/{topic}"
            )
            parameters = {
                "path": full_path,
                "selectedFields": "WebId;Name"  # added Name
            }
            get_request += "?" + "&".join(
                [f"{key}={value}" for key, value in parameters.items()]
            )
            response = self.__make_get_request(get_request)
            webid = response.get("WebId")
            name = response.get("Name")
            if not webid:
                self.logger.warning(f"No WebId found for exact topic: {topic}")
                return {}
            
            prefix = f"{self.__secrets['mqtt_connector']['path']}/"
            topic_name = name.replace(prefix, "", 1) if name else topic
            self.logger.info(f"Resolved '{topic}' to WebId: {webid}")
            return {webid: topic_name}

        
    def __make_get_request(self, url):
        
        response = self.session.get(url)
        
        if response.status_code != 200:
            self.logger.error(f"GET request failed. URL: {url}, Status Code: {response.status_code}, Response: {response.text}")
            raise GrpcError.InternalError(f"GET request failed. Status Code: {response.status_code}")
        return response.json()