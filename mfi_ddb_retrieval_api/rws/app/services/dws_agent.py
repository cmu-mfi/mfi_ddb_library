"""
DWS agent: fetches datapoints for trial topics from the database nodes (DWS).

For each topic family (first topic level, e.g. "mfi-v1.0-historian") the
configured DWS servers are asked for every datapoint in [time_start, time_end]:

  1. StreamDataRange (preferred): the server streams all datapoints in batches
     and ends the stream - one query, nothing skipped, no page tokens.
  2. GetDataRange (fallback, for servers that answer UNIMPLEMENTED, e.g. the
     AVEVA PI and blob nodes): page through with page tokens.

Only exact duplicates (e.g. the same datapoint from two servers of a family)
are dropped, timestamps keep their full precision, and one failing server
doesn't lose the data of the others.
"""

import copy
import logging
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Union

import grpc
import yaml
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

from app.utils.dws.gen.models_pb2 import Datapoint
from app.utils.dws.gen.service_pb2 import (
    GetDataRangeRequest,
    GetDataRangeResponse,
    StreamDataRangeRequest,
)
from app.utils.dws.gen.service_pb2_grpc import DataServiceStub

logger = logging.getLogger(__name__)

PAGE_SIZE = 1000            # GetDataRange fallback page size
STREAM_BATCH_SIZE = 5000    # datapoints per StreamDataRange message
CHANNEL_OPTIONS = [
    # Headroom above gRPC's 4 MB default for large pages / batches.
    ("grpc.max_receive_message_length", 64 * 1024 * 1024),
]


def _to_timestamp(value: str) -> Timestamp:
    """ISO 8601 string -> protobuf Timestamp, full precision. Naive times are UTC."""
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def _value_of(dp: Datapoint):
    field = dp.WhichOneof("value")
    if field == "int_value":
        return dp.int_value
    if field == "float_value":
        return dp.float_value
    if field == "string_value":
        return dp.string_value
    if field == "json_value":
        return MessageToDict(dp.json_value)
    if field == "file_value":
        logger.warning(
            "File value found for topic %s at %s; returning the filename instead of the content.",
            dp.topic, dp.timestamp.ToJsonString(),
        )
        return dp.file_value.filename
    return None


class __DwsAgent:
    def __init__(self, cfg_file: str = "dws.endpoints.yaml"):
        current_dir = Path(__file__).parent
        dws_config_path = Path(current_dir, "../config", cfg_file)

        self.config = self._load_config(dws_config_path)
        self._channels: Dict[str, grpc.Channel] = {}
        self._channels_lock = threading.Lock()

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

    # ------------------------------------------------------------------
    # public
    # ------------------------------------------------------------------

    def get_data(self, topics: Union[str, List[str]], time_start: str, time_end: str) -> Dict:
        """{topic: [{"timestamp": ISO 8601 UTC, "value": ...}, ...]} ordered by time."""
        if isinstance(topics, str):
            topics = [topics]

        topic_family_map: Dict[str, List[str]] = {}
        for topic in topics:
            topic_family = topic.split("/")[0]
            topic_family_map.setdefault(topic_family, []).append(topic)

        start_ts = _to_timestamp(time_start)
        end_ts = _to_timestamp(time_end)

        collected: Dict[str, list] = {}
        seen = set()
        for topic_family, family_topics in topic_family_map.items():
            if topic_family not in getattr(self, "config", {}):
                logger.error(
                    "Topic family '%s' not found in DWS configuration. Skipping topics: %s",
                    topic_family, family_topics,
                )
                continue

            topic = ",".join(family_topics)
            for server in self.config[topic_family]:
                logger.info("Retrieving %s from %s at %s", family_topics, server["name"], server["url"])
                try:
                    for dp in self._fetch_from_server(server["url"], topic, start_ts, end_ts):
                        # Drop only exact duplicates (same datapoint from two servers).
                        key = dp.SerializeToString(deterministic=True)
                        if key in seen:
                            continue
                        seen.add(key)
                        collected.setdefault(dp.topic, []).append(
                            ((dp.timestamp.seconds, dp.timestamp.nanos), _value_of(dp))
                        )
                except grpc.RpcError as err:
                    logger.error(
                        "DWS %s (%s) failed: %s %s",
                        server["name"], server["url"], err.code(), err.details(),
                    )
                except Exception:
                    logger.exception("DWS %s (%s) failed", server["name"], server["url"])

        result = {}
        for topic, items in collected.items():
            items.sort(key=lambda item: item[0])
            result[topic] = [
                {
                    "timestamp": datetime.fromtimestamp(sec + nanos / 1e9, tz=timezone.utc).isoformat(),
                    "value": value,
                }
                for (sec, nanos), value in items
            ]
        return result

    # ------------------------------------------------------------------
    # DWS calls
    # ------------------------------------------------------------------

    def _fetch_from_server(
        self, endpoint: str, topic: str, start_ts: Timestamp, end_ts: Timestamp
    ) -> Iterator[Datapoint]:
        """Stream the window if the server supports it, otherwise page through it."""
        stream_request = StreamDataRangeRequest(
            topic=topic, start_time=start_ts, end_time=end_ts, batch_size=STREAM_BATCH_SIZE
        )
        try:
            # Materialize the first batch here so UNIMPLEMENTED surfaces before
            # anything is yielded and the fallback can take over cleanly.
            batches = iter(self._stream_dws_server(endpoint, stream_request))
            first = next(batches, None)
        except grpc.RpcError as err:
            if err.code() != grpc.StatusCode.UNIMPLEMENTED:
                raise
            logger.info("DWS at %s has no StreamDataRange; paging with GetDataRange", endpoint)
            yield from self._page_dws_server(endpoint, topic, start_ts, end_ts)
            return
        if first is not None:
            yield from first
            for batch in batches:
                yield from batch

    def _page_dws_server(
        self, endpoint: str, topic: str, start_ts: Timestamp, end_ts: Timestamp
    ) -> Iterator[Datapoint]:
        request = GetDataRangeRequest(
            topic=topic, start_time=start_ts, end_time=end_ts, page_size=PAGE_SIZE, page_token=""
        )
        response: GetDataRangeResponse = self._call_dws_server(endpoint, request)
        yield from response.datapoints
        while response.next_page_token != "":
            request = copy.deepcopy(request)
            request.page_token = response.next_page_token
            response = self._call_dws_server(endpoint, request)
            yield from response.datapoints

    def _channel(self, endpoint: str) -> grpc.Channel:
        """One reusable channel per endpoint (gRPC channels are thread-safe)."""
        with self._channels_lock:
            channel = self._channels.get(endpoint)
            if channel is None:
                channel = grpc.insecure_channel(endpoint, options=CHANNEL_OPTIONS)
                self._channels[endpoint] = channel
            return channel

    def _stream_dws_server(self, endpoint: str, request: StreamDataRangeRequest) -> Iterable[List[Datapoint]]:
        """Yield the datapoint batches of a StreamDataRange call."""
        stub = DataServiceStub(self._channel(endpoint))
        for message in stub.StreamDataRange(request):
            yield list(message.datapoints)

    def _call_dws_server(self, endpoint: str, request: GetDataRangeRequest) -> GetDataRangeResponse:
        stub = DataServiceStub(self._channel(endpoint))
        return stub.GetDataRange(request)


DwsAgent = __DwsAgent()
