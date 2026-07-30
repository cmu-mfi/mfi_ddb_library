import copy
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Union, Dict

import grpc
import yaml
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

from app.utils.dws.gen.models_pb2 import Datapoint
from app.utils.dws.gen.service_pb2 import (
    GetDataRangeRequest,
    GetDataRangeResponse,
)
from app.utils.dws.gen.service_pb2_grpc import DataServiceStub

from opentelemetry.metrics import get_meter

meter = get_meter("mfi.rws.app")

dws_retrieval_duration = meter.create_histogram(
    "mfi_rws_dws_retrieval_duration_seconds",
    description="Time spent retrieving actual timeseries points via DwsAgent"
)

dws_retrieval_counter = meter.create_counter(
    "mfi_rws_dws_retrievals_total",
    description="Total raw data extraction runs requested by users"
)

# --- Sub-operation Timers ---
dws_grpc_call_duration = meter.create_histogram(
    "mfi_rws_dws_grpc_call_duration_seconds",
    description="Duration of raw gRPC network calls including pagination"
)

dws_dedup_duration = meter.create_histogram(
    "mfi_rws_dws_dedup_duration_seconds",
    description="Duration of in-memory deduplication"
)

dws_serialization_duration = meter.create_histogram(
    "mfi_rws_dws_serialization_duration_seconds",
    description="Duration of Protobuf to Python Dict conversion"
)

logger = logging.getLogger(__name__)


class __DwsAgent:
    def __init__(self, cfg_file: str = "dws.endpoints.yaml"):
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
        start_time = time.perf_counter()
        primary_topic = topics[0] if isinstance(topics, list) and len(topics) > 0 else (topics or "unknown")

        if isinstance(topics, str):
            topics = [topics]
            
        try:
            topic_family_map = {}
            for topic in topics:
                topic_family = topic.split("/")[0]
                topic_family_map[topic_family] = topic_family_map.get(topic_family, []) + [topic]
            
            result = {}
            for topic_family in list(topic_family_map.keys()):
                if topic_family not in self.config:
                    logger.error(f"Topic family '{topic_family}' not found in DWS configuration. Skipping: {topic_family_map[topic_family]}")
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
                
                # ------------------------------------------------------------------
                # 1. MEASURE gRPC FETCH & PAGINATION TIME
                # ------------------------------------------------------------------
                for server in servers:
                    grpc_start = time.perf_counter()
                    logger.info(f"Retrieving data from server {server['name']} ({server['url']})")
                    
                    page_count = 0
                    # Open a single persistent channel per server instead of recreating per request
                    with grpc.insecure_channel(server['url']) as channel:
                        stub = DataServiceStub(channel)
                        
                        response: GetDataRangeResponse = stub.GetDataRange(request)
                        raw_data = response.datapoints
                        page_count += 1

                        while response.next_page_token != "":
                            request = copy.deepcopy(request)
                            request.page_token = response.next_page_token
                            response = stub.GetDataRange(request)
                            raw_data.extend(response.datapoints)
                            page_count += 1
                        
                    data_points.extend(raw_data)
                    
                    grpc_duration = time.perf_counter() - grpc_start
                    dws_grpc_call_duration.record(
                        grpc_duration, 
                        {"server": server['name'], "pages": str(page_count)}
                    )
                    logger.info(f"gRPC fetch from {server['name']} took {grpc_duration:.3f}s across {page_count} page(s) ({len(raw_data)} datapoints)")

                # ------------------------------------------------------------------
                # 2. MEASURE DEDUPLICATION TIME
                # ------------------------------------------------------------------
                dedup_start = time.perf_counter()
                seen = set()
                unique_data_points: List[Datapoint] = []
                for dp in data_points:
                    key = (dp.topic, dp.timestamp.seconds)
                    if key not in seen:
                        seen.add(key)
                        unique_data_points.append(dp)
                
                dedup_duration = time.perf_counter() - dedup_start
                dws_dedup_duration.record(dedup_duration, {"topic_family": topic_family})
                logger.info(f"Deduplication took {dedup_duration:.3f}s for {len(data_points)} total -> {len(unique_data_points)} unique points")

                # ------------------------------------------------------------------
                # 3. MEASURE PROTOBUF TO DICT SERIALIZATION TIME
                # ------------------------------------------------------------------
                serial_start = time.perf_counter()
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
                        logger.warning(f"File value found for topic {dp.topic}. Returning filename '{value}'")
                    
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
                
                serial_duration = time.perf_counter() - serial_start
                dws_serialization_duration.record(serial_duration, {"topic_family": topic_family})
                logger.info(f"Serialization took {serial_duration:.3f}s for {len(unique_data_points)} points")

            # Overall Metric Recording
            duration = time.perf_counter() - start_time
            dws_retrieval_duration.record(duration, {"trial_name": primary_topic, "status": "SUCCESS"})
            dws_retrieval_counter.add(1, {"trial_name": primary_topic, "status": "SUCCESS"})

            return result

        except Exception as e:
            duration = time.perf_counter() - start_time
            dws_retrieval_duration.record(duration, {"trial_name": primary_topic, "status": "ERROR"})
            dws_retrieval_counter.add(1, {"trial_name": primary_topic, "status": "ERROR"})
            raise e


DwsAgent = __DwsAgent()