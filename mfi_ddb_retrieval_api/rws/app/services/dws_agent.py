import logging
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from functools import lru_cache
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

# ==============================================================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==============================================================================
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
# ==============================================================================

logger = logging.getLogger(__name__)


def parse_datetime(ts_str: str) -> datetime:
    """Safely convert ISO string to timezone-naive UTC datetime for arithmetic."""
    dt = datetime.fromisoformat(ts_str)
    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


@lru_cache(maxsize=10000)
def cached_iso_format(ts_seconds: int) -> str:
    """Cache timestamp conversion to avoid repetitive datetime construction."""
    return datetime.fromtimestamp(ts_seconds, tz=timezone.utc).isoformat()


class __DwsAgent:
    def __init__(self, cfg_file: str = "dws.endpoints.yaml"):
        current_dir = Path(__file__).parent
        dws_config_path = Path(current_dir, "../config", cfg_file)
        
        self.config = self._load_config(dws_config_path)
        # Persistent channel & stub storage for connection reuse
        self._channels: Dict[str, grpc.Channel] = {}
        self._stubs: Dict[str, DataServiceStub] = {}
        
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

    def _get_stub(self, server_url: str) -> DataServiceStub:
        """Reuse gRPC channels and stubs across worker threads."""
        if server_url not in self._stubs:
            # Configure gRPC channel with keepalive options to maintain persistent connection
            options = [
                ('grpc.keepalive_time_ms', 30000),
                ('grpc.keepalive_timeout_ms', 10000),
                ('grpc.keepalive_permit_without_calls', 1),
                ('grpc.http2.max_pings_without_data', 0),
            ]
            channel = grpc.insecure_channel(server_url, options=options)
            self._channels[server_url] = channel
            self._stubs[server_url] = DataServiceStub(channel)
        return self._stubs[server_url]

    def _fetch_time_chunk(self, stub: DataServiceStub, topics_str: str, start_dt: datetime, end_dt: datetime) -> List[Datapoint]:
        """Worker thread function using a SHARED gRPC stub."""
        request = GetDataRangeRequest(
            topic=topics_str,
            start_time=Timestamp(seconds=int(start_dt.timestamp())),
            end_time=Timestamp(seconds=int(end_dt.timestamp())),
            page_size=2500,
            page_token=""
        )
        
        datapoints = []
        response: GetDataRangeResponse = stub.GetDataRange(request)
        datapoints.extend(response.datapoints)
        
        page_count = 1
        while response.next_page_token != "":
            request.page_token = response.next_page_token
            response = stub.GetDataRange(request)
            datapoints.extend(response.datapoints)
            page_count += 1
            
        return datapoints

    def get_data(self, topics: Union[str, List[str]], time_start: str, time_end: str, max_workers: int = 10) -> Dict:
        start_time = time.perf_counter()
        primary_topic = topics[0] if isinstance(topics, list) and len(topics) > 0 else (topics or "unknown")

        if isinstance(topics, str):
            topics = [topics]
            
        try:
            topic_family_map = defaultdict(list)
            for topic in topics:
                topic_family = topic.split("/")[0]
                topic_family_map[topic_family].append(topic)
            
            result = defaultdict(list)
            dt_start = parse_datetime(time_start)
            dt_end = parse_datetime(time_end)

            for topic_family, family_topics in topic_family_map.items():
                if topic_family not in self.config:
                    logger.error(f"Topic family '{topic_family}' not found in DWS configuration.")
                    continue
                
                topics_str = ",".join(family_topics)
                servers = self.config[topic_family]
                data_points: List[Datapoint] = []
                
                # ------------------------------------------------------------------
                # 1. PARALLEL CHUNKED gRPC FETCH (REUSING CHANNELS)
                # ------------------------------------------------------------------
                grpc_start = time.perf_counter()
                
                total_duration_sec = (dt_end - dt_start).total_seconds()
                chunk_duration_sec = total_duration_sec / max_workers
                
                time_chunks = []
                for i in range(max_workers):
                    chunk_start = dt_start + timedelta(seconds=i * chunk_duration_sec)
                    chunk_end = dt_start + timedelta(seconds=(i + 1) * chunk_duration_sec) if i < max_workers - 1 else dt_end
                    time_chunks.append((chunk_start, chunk_end))

                for server in servers:
                    logger.info(f"Retrieving data in parallel across {max_workers} threads from server: {server['name']}")
                    
                    # Get persistent stub for this server
                    stub = self._get_stub(server['url'])
                    
                    with ThreadPoolExecutor(max_workers=max_workers) as executor:
                        futures = [
                            executor.submit(
                                self._fetch_time_chunk, 
                                stub, 
                                topics_str, 
                                c_start, 
                                c_end
                            )
                            for c_start, c_end in time_chunks
                        ]
                        
                        for future in as_completed(futures):
                            data_points.extend(future.result())

                    grpc_duration = time.perf_counter() - grpc_start
                    dws_grpc_call_duration.record(
                        grpc_duration, 
                        {"server": server['name'], "topic_family": topic_family}
                    )

                # ------------------------------------------------------------------
                # 2. DEDUPLICATION
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

                # ------------------------------------------------------------------
                # 3. SERIALIZATION
                # ------------------------------------------------------------------
                serial_start = time.perf_counter()
                for dp in unique_data_points:
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
                    else:
                        value = None

                    result[dp.topic].append({
                        "timestamp": cached_iso_format(dp.timestamp.seconds),
                        "value": value
                    })

                serial_duration = time.perf_counter() - serial_start
                dws_serialization_duration.record(serial_duration, {"topic_family": topic_family})

                # Detailed breakdown logging as suggested by ChatGPT
                logger.info(
                    f"\n--- PERFORMANCE BREAKDOWN ---\n"
                    f"Points Returned : {len(unique_data_points)}\n"
                    f"gRPC Fetch      : {grpc_duration:.3f}s\n"
                    f"Deduplication   : {dedup_duration:.3f}s\n"
                    f"Serialization   : {serial_duration:.3f}s\n"
                    f"Total Run       : {time.perf_counter() - start_time:.3f}s\n"
                    f"-----------------------------"
                )

            duration = time.perf_counter() - start_time
            dws_retrieval_duration.record(duration, {"trial_name": primary_topic, "status": "SUCCESS"})
            dws_retrieval_counter.add(1, {"trial_name": primary_topic, "status": "SUCCESS"})

            # Convert defaultdict back to standard dict
            return dict(result)

        except Exception as e:
            logger.exception("Error executing get_data in DwsAgent")
            duration = time.perf_counter() - start_time
            dws_retrieval_duration.record(duration, {"trial_name": primary_topic, "status": "ERROR"})
            dws_retrieval_counter.add(1, {"trial_name": primary_topic, "status": "ERROR"})
            raise e

    def __del__(self):
        """Cleanup gRPC channels when the agent instance is destroyed."""
        for channel in getattr(self, '_channels', {}).values():
            channel.close()


DwsAgent = __DwsAgent()