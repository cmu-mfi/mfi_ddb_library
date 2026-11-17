import copy
import hashlib
import logging
import tempfile
from datetime import datetime
from pathlib import Path
from typing import Any, Literal, Optional, Union

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


class __DwsAgent:
    def __init__(self, cfg_file: str = "dws.endpoints.yaml"):
        current_dir = Path(__file__).parent
        dws_config_path = Path(current_dir, "../config", cfg_file)

        self.config = self._load_config(dws_config_path)

    def _load_config(self, config_file):
        with open(config_file, "r") as f:
            data = yaml.safe_load(f)

        if "services" not in data:
            raise ValueError("Invalid DWS configuration: 'services' key not found.")

        config = {}
        for service in data["services"]:
            service_cfg = data["services"][service]
            service_cfg["name"] = service
            for topic_family in service_cfg["topic_families"]:
                if topic_family not in config:
                    config[topic_family] = []
                config[topic_family].append(service_cfg)
        return config

    def get_data(
        self,
        topics: Union[str, list[str]],
        time_start: str,
        time_end: str,
        bytes: bool = False,
    ) -> dict[str, list[dict[Literal["timestamp", "value"], Any]]]:

        if isinstance(topics, str):
            topics = [topics]

        topic_family_map = {}
        for topic in topics:
            topic_family = topic.split("/")[0]
            topic_family_map[topic_family] = topic_family_map.get(topic_family, []) + [
                topic
            ]

        result = {}
        for topic_family in list(topic_family_map.keys()):
            if topic_family not in self.config:
                logger.error(
                    f"Topic family '{topic_family}' not found in DWS configuration. Skipping data retrieval for topics: {topic_family_map[topic_family]}"
                )
                continue

            request = GetDataRangeRequest(
                topic=",".join(topic_family_map[topic_family]),
                start_time=Timestamp(
                    seconds=int(datetime.fromisoformat(time_start).timestamp())
                ),
                end_time=Timestamp(
                    seconds=int(datetime.fromisoformat(time_end).timestamp())
                ),
                page_size=1000,
                page_token="",
            )

            servers = self.config[topic_family]
            data_points: list[Datapoint] = []
            for server in servers:
                logger.info(
                    f"Retrieving data for topics: {topic_family_map[topic_family]} from server: {server['name']} at {server['url']}"
                )
                response: GetDataRangeResponse = self._call_dws_server(
                    server["url"], request
                )
                raw_data = response.datapoints

                while response.next_page_token != "":
                    request = copy.deepcopy(request)
                    request.page_token = response.next_page_token
                    response: GetDataRangeResponse = self._call_dws_server(
                        server["url"], request
                    )
                    raw_data.extend(response.datapoints)

                data_points.extend(raw_data)

            # REMOVE DUPLICATES BASED ON TOPIC AND TIMESTAMP
            seen = set()
            unique_data_points: list[Datapoint] = []
            for dp in data_points:
                key = (dp.topic, dp.timestamp.seconds)
                if key not in seen:
                    seen.add(key)
                    unique_data_points.append(dp)

            # CONVERT TO KEY VALUE PAIRS
            # When bytes=True, file values are saved to a temporary directory
            # created lazily for this call; cleanup is the caller's
            # responsibility (see the RWS type4 endpoint).
            temp_dir: Optional[Path] = None
            file_count = 0
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
                    if not bytes:
                        value = dp.file_value.filename
                    else:
                        if temp_dir is None:
                            temp_dir = Path(tempfile.mkdtemp(prefix="rws_files_"))

                        content = dp.file_value.content
                        sha256_expected = dp.file_value.sha256 or None
                        if sha256_expected:
                            sha256_actual = hashlib.sha256(content).hexdigest()
                            if sha256_actual != sha256_expected:
                                logger.warning(
                                    f"SHA256 mismatch for file '{dp.file_value.filename}' "
                                    f"on topic {dp.topic} at timestamp {dp.timestamp.seconds}: "
                                    f"expected {sha256_expected}, got {sha256_actual}"
                                )

                        # All files of one call live flat in the same temp dir;
                        # an index prefix keeps same-named files from colliding.
                        # The original filename is returned alongside via the
                        # "filename" key.
                        filepath = temp_dir / f"{file_count:04d}_{Path(dp.file_value.filename).name}"
                        filepath.write_bytes(content)
                        value = str(filepath)
                        file_count += 1

                entry = {
                    "timestamp": datetime.fromtimestamp(
                        dp.timestamp.seconds
                    ).isoformat(),
                    "value": value,
                }
                if value_field == "file_value" and bytes:
                    entry["filename"] = dp.file_value.filename

                if dp.topic in result:
                    result[dp.topic].append(entry)
                else:
                    result[dp.topic] = [entry]
        return result

    def _call_dws_server(
        self, endpoint: str, request: GetDataRangeRequest
    ) -> GetDataRangeResponse:
        with grpc.insecure_channel(endpoint) as channel:
            stub = DataServiceStub(channel)
            response = stub.GetDataRange(request)
            return response


DwsAgent = __DwsAgent()
