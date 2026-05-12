import copy
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union

import grpc
import yaml
from google.protobuf.timestamp_pb2 import Timestamp

from app.utils.dws.gen.models_pb2 import Datapoint
from app.utils.dws.gen.service_pb2 import (
    GetDataPointRequest,
    GetDataPointResponse,
    GetDataRangeRequest,
    GetDataRangeResponse,
)
from app.utils.dws.gen.service_pb2_grpc import DataServiceStub

logger = logging.getLogger(__name__)
class __DwsAgent:
    def __init__(self):
        current_dir = Path(__file__).parent
        dws_config_path = Path(current_dir, "../config","dws_config.yaml")
        
        self.config = self._load_config(dws_config_path)
        
    def _load_config(self, config_file):
        with open(config_file, 'r') as f:
            data = yaml.safe_load(f)
            
        if "services" not in data:
            raise ValueError("Invalid DWS configuration: 'services' key not found.")
        
        config = {}
        for service in data['services']:
            for topic_family in service['topic_families']:
                if topic_family not in config:
                    config[topic_family] = []
                config[topic_family].append(service)
        return config   

    def get_data(self, topics: Union[str, List[str]], time_start: str, time_end: str):
        
        if isinstance(topics, str):
            topics = [topics]
            
        topic_family_map = {}
        for topic in topics:
            topic_family = topic.split("/")[0]
            topic_family_map[topic_family] = topic_family_map.get(topic_family, []) + [topic]
        
        for topic_family in list(topic_family_map.keys()):
            if topic_family not in self.config:
                logger.error(f"Topic family '{topic_family}' not found in DWS configuration. Skipping data retrieval for topics: {topic_family_map[topic_family]}")
                continue
                        
            request = GetDataRangeRequest(
                topic=",".join(topic_family_map[topic_family]),
                start_time=Timestamp(seconds=int(datetime.fromisoformat(time_start).timestamp())),
                end_time=Timestamp(seconds=int(datetime.fromisoformat(time_end).timestamp())),
                page_size=1000,
                page_token=""
            )
            
            servers = self.config[topic_family]
            data_points: List[Datapoint] = []
            for server in servers:
                logger.info(f"Retrieving data for topics: {topic_family_map[topic_family]} from server: {server['name']} at {server['endpoint']}")
                response: GetDataRangeResponse = self._call_dws_server(server['endpoint'], request)
                raw_data = response.datapoints
                
                while response.next_page_token != "":
                    request_2 = copy.deepcopy(request)
                    request_2.page_token = response.next_page_token
                    response_2: GetDataRangeResponse = self._call_dws_server(server['endpoint'], request_2)
                    raw_data.extend(response_2.datapoints)
                    
                data_points.extend(raw_data)
                
            # REMOVE DUPLICATES BASED ON TOPIC AND TIMESTAMP
            seen = set()
            unique_data_points: List[Datapoint] = []
            for dp in data_points:
                key = (dp.topic, dp.timestamp.seconds)
                if key not in seen:
                    seen.add(key)
                    unique_data_points.append(dp)
                    
            # CONVERT TO KEY VALUE PAIRS
            result = {}
            for dp in unique_data_points:
                value = None
                if dp.int_value != 0:
                    value = dp.int_value
                elif dp.float_value != 0.0:
                    value = dp.float_value
                elif dp.string_value != "":
                    value = dp.string_value
                elif dp.json_value != {}:
                    value = dp.json_value
                elif dp.file_value != b"":
                    value = dp.file_value.filename
                    logger.warning(f"File value found for topic {dp.topic} at timestamp {dp.timestamp.seconds}. Returning filename '{value}' instead of file content.")
                
                if dp.topic in result:
                    result[dp.topic].append({
                        "timestamp": datetime.fromtimestamp(dp.timestamp.seconds).isoformat(),
                        "value": value
                    })
                else:
                    result[dp.topic] = [{
                        "timestamp": datetime.fromtimestamp(dp.timestamp.seconds).isoformat(),
                        "value": value
                    }]
            return result
        
    def _call_dws_server(self, endpoint: str, request: GetDataRangeRequest) -> GetDataRangeResponse:
        with grpc.insecure_channel(endpoint) as channel:
            stub = DataServiceStub(channel)
            response = stub.GetDataRange(request)
            return response