# tests/test_server.py
from concurrent import futures
from datetime import datetime, timezone
from pathlib import Path
import sys

import grpc
import pytest
import yaml
from google.protobuf.timestamp_pb2 import Timestamp

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from dws import server
from dws.gen import service_pb2, service_pb2_grpc


@pytest.fixture(scope="module")
def config():
    secrets_path = ROOT / "dws" / "config.yaml"
    with open(secrets_path, "r") as f:
        return yaml.safe_load(f)


def to_timestamp(value: str) -> Timestamp:
    dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


TEST_CASES = [
    (
        r"mfi-v1.0-historian/Mill-19/AV-Room/telemetry/#",
        "2026-03-17 00:00:00",
        "2025-10-21 10:00:00",
        "2025-10-21 10:01:00",
    ),
    (
        r"mfi-v1.0-historian/Mill-19/Server-Room/telemetry/DATA/data/temperature",
        "2026-03-17 00:00:00",
        "2026-03-17 00:00:00",
        "2026-03-17 01:00:00",
    ),
]


@pytest.fixture(scope="module")
def grpc_server():
    srv = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    service_pb2_grpc.add_DataServiceServicer_to_server(server.DataService(), srv)

    port = srv.add_insecure_port("127.0.0.1:0")
    srv.start()

    yield f"127.0.0.1:{port}"

    srv.stop(0)


@pytest.fixture(scope="module")
def stub(grpc_server):
    channel = grpc.insecure_channel(grpc_server)
    grpc.channel_ready_future(channel).result(timeout=10)
    return service_pb2_grpc.DataServiceStub(channel)


@pytest.mark.parametrize("topic,timestamp,start_time,end_time", TEST_CASES)
def test_get_data_point(stub, topic, timestamp, start_time, end_time):
    request = service_pb2.GetDataPointRequest(
        topic=topic,
        timestamp=to_timestamp(timestamp),
    )

    if topic.endswith("/#"):
        with pytest.raises(grpc.RpcError) as exc:
            stub.GetDataPoint(request)

        assert exc.value.code() == grpc.StatusCode.NOT_FOUND
        assert exc.value.details() == "No datapoint found"
        return

    response = stub.GetDataPoint(request)

    assert response is not None
    assert response.HasField("datapoint")
    assert response.datapoint.topic
    assert response.datapoint.timestamp.seconds > 0


@pytest.mark.parametrize("topic,timestamp,start_time,end_time", TEST_CASES)
def test_get_data_range(stub, topic, timestamp, start_time, end_time):
    request = service_pb2.GetDataRangeRequest(
        topic=topic,
        start_time=to_timestamp(start_time),
        end_time=to_timestamp(end_time),
        page_size=10,
        page_token="",
    )

    response = stub.GetDataRange(request)

    assert response is not None
    assert len(response.datapoints) <= 10
    for dp in response.datapoints:
        assert dp.topic
        assert dp.timestamp.seconds > 0