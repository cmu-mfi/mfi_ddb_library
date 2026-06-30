"""Quick DWS smoke client for TimeScaleDB."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

from gen import service_pb2, service_pb2_grpc

DWS_ADDR = "localhost:50051"
TOPIC = "mfi-v1.0-historian/CMU/DDATA/Machine Shop/robot-arm-1"


def to_pb_ts(dt: datetime) -> Timestamp:
    ts = Timestamp()
    ts.FromDatetime(dt)
    return ts


def main() -> None:
    channel = grpc.insecure_channel(DWS_ADDR)
    stub = service_pb2_grpc.DataServiceStub(channel)

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=1)

    # GetDataPoint (closest past)
    # dp_req = service_pb2.GetDataPointRequest(
    #     topic=TOPIC,
    #     timestamp=to_pb_ts(now),
    # )
    # dp_resp = stub.GetDataPoint(dp_req)
    # print("GetDataPoint:", dp_resp)

    # GetDataRange
    # range_req = service_pb2.GetDataRangeRequest(
    #     topic=TOPIC,
    #     start_time=to_pb_ts(start),
    #     end_time=to_pb_ts(now),
    #     page_size=10,
    #     page_token="",
    # )
    # range_resp = stub.GetDataRange(range_req)
    # print("GetDataRange:", range_resp)

    # StreamData (looped)
    stream_req = service_pb2.StreamDataRequest(
        topic=TOPIC,
        start_from=to_pb_ts(start),
    )
    for resp in stub.StreamData(stream_req):
        # Stream blocks until new rows are available.
        print("StreamData:", resp)


if __name__ == "__main__":
    main()
