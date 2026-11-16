import copy
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union, Dict
from urllib.parse import urlsplit

import grpc
import yaml
from google.protobuf.json_format import MessageToDict
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


class DwsReadError(RuntimeError):
    """A source could not provide a complete, bounded retrieval."""


def topic_matches(pattern, topic):
    wanted, actual = pattern.split("/"), topic.split("/")
    for index, part in enumerate(wanted):
        if part == "#":
            return index == len(wanted) - 1
        if index >= len(actual) or (part != "+" and part != actual[index]):
            return False
    return len(wanted) == len(actual)


class __DwsAgent:
    def __init__(self, cfg_file:str = "dws.endpoints.yaml"):
        current_dir = Path(__file__).parent
        dws_config_path = Path(current_dir, "../config", cfg_file)
        
        self.config = self._load_config(dws_config_path)
        
    def _load_config(self, config_file):
        with open(config_file, 'r') as f:
            data = yaml.safe_load(f)
            
        if "services" not in data:
            raise ValueError("Invalid DWS configuration: 'services' key not found.")
        
        config = {}
        for service in data['services']:
            service_cfg = data['services'][service]
            service_cfg['name'] = service
            for topic_family in service_cfg['topic_families']:
                if topic_family not in config:
                    config[topic_family] = []
                config[topic_family].append(service_cfg)
        return config   

    def get_data(self, topics: Union[str, List[str]], time_start: str, time_end: str) -> Dict:
        
        if isinstance(topics, str):
            topics = [topics]
            
        topic_family_map = {}
        for topic in topics:
            topic_family = topic.split("/")[0]
            topic_family_map[topic_family] = topic_family_map.get(topic_family, []) + [topic]
        
        result = {}
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
                logger.info(f"Retrieving data for topics: {topic_family_map[topic_family]} from server: {server['name']} at {server['url']}")
                response: GetDataRangeResponse = self._call_dws_server(server['url'], request)
                raw_data = response.datapoints

                while response.next_page_token != "":
                    request = copy.deepcopy(request)
                    request.page_token = response.next_page_token
                    response: GetDataRangeResponse = self._call_dws_server(server['url'], request)
                    raw_data.extend(response.datapoints)
                    
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
            for dp in unique_data_points:
                value = None
                value_field = dp.WhichOneof("value")
                if value_field == "int_value":
                    value = dp.int_value
                elif value_field == "float_value":
                    value = dp.float_value
                elif value_field == "string_value":
                    value = dp.string_value
                elif value_field == "json_value":
                    value = MessageToDict(dp.json_value)
                elif value_field == "file_value":
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
        
    def get_data_bounded(self, topics, time_start, time_end, *, max_points=100000, max_pages=1000, timeout_seconds=60):
        """Strict path for AI: individual topics, deadlines, limits and precise timestamps.

        Fail rather than index a truncated or partially unavailable experiment.
        Each source/topic gets its own pagination cursor.
        """
        from datetime import timezone

        def timestamp(value):
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            result = Timestamp()
            result.FromDatetime(dt)
            return result

        start, end = timestamp(time_start), timestamp(time_end)
        lower, upper = start.ToNanoseconds(), end.ToNanoseconds()
        if upper < lower:
            raise ValueError("time_end must be at or after time_start")
        deadline = time.monotonic() + timeout_seconds
        pages, count, seen, result = 0, 0, {}, {}
        for topic in sorted(set(topics)):
            servers = self.config.get(topic.split("/")[0])
            if not servers:
                raise DwsReadError(f"No DWS configured for topic family {topic.split('/')[0]}")
            for server in servers:
                token, tokens = "", set()
                while True:
                    if time.monotonic() >= deadline:
                        raise DwsReadError("DWS retrieval time budget exceeded")
                    pages += 1
                    if pages > max_pages:
                        raise DwsReadError("DWS page limit exceeded; no partial index was committed")
                    request = GetDataRangeRequest(topic=topic, start_time=start, end_time=end,
                                                  page_size=min(1000, max_points), page_token=token)
                    response = self._call_dws_server(server["url"], request)
                    for point in response.datapoints:
                        count += 1
                        if count > max_points:
                            raise DwsReadError("DWS point limit exceeded; narrow the time window or increase DDB_AI_MAX_POINTS")
                        if not topic_matches(topic, point.topic) or not lower <= point.timestamp.ToNanoseconds() <= upper:
                            raise DwsReadError("DWS returned data outside the authorized topic/time window")
                        kind = point.WhichOneof("value")
                        if kind == "file_value":
                            raise DwsReadError("File-content indexing is outside the experiment RAG scope")
                        if kind == "json_value":
                            value = MessageToDict(point.json_value)
                        elif kind in ("int_value", "float_value", "string_value"):
                            value = getattr(point, kind)
                        else:
                            continue
                        key = (point.topic, point.timestamp.seconds, point.timestamp.nanos)
                        if key in seen:
                            if seen[key] != value:
                                raise DwsReadError("DWS sources disagree on a topic/timestamp value")
                            continue
                        seen[key] = value
                        result.setdefault(point.topic, []).append({"timestamp": point.timestamp.ToJsonString(), "value": value})
                    token = response.next_page_token
                    if not token:
                        break
                    if token in tokens:
                        raise DwsReadError("DWS repeated a pagination cursor")
                    tokens.add(token)
        for points in result.values():
            points.sort(key=lambda p: datetime.fromisoformat(p["timestamp"].replace("Z", "+00:00")))
        return result

    def _call_dws_server(self, endpoint: str, request: GetDataRangeRequest) -> GetDataRangeResponse:
        parsed = urlsplit(endpoint if "://" in endpoint else "grpc://" + endpoint)
        target = parsed.netloc
        if not parsed.hostname or not parsed.port or parsed.path not in ("", "/"):
            raise ValueError("DWS endpoint must identify a host and port")
        channel_factory = grpc.secure_channel if parsed.scheme in ("https", "grpcs") else grpc.insecure_channel
        args = (target, grpc.ssl_channel_credentials()) if channel_factory == grpc.secure_channel else (target,)
        with channel_factory(*args) as channel:
            stub = DataServiceStub(channel)
            response = stub.GetDataRange(request, timeout=10)
            return response
        
DwsAgent = __DwsAgent()
