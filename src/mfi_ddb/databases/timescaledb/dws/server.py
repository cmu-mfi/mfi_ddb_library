"""DWS gRPC server for TimeScaleDB historian retrieval."""

import json
import time
from datetime import datetime, timezone
from pathlib import Path
import grpc
from concurrent import futures
import yaml
from google.protobuf.timestamp_pb2 import Timestamp
from google.protobuf.struct_pb2 import Struct

from .db import TimeScaleReader
from .gen import models_pb2, service_pb2, service_pb2_grpc

def load_config(path: Path):
    """Load YAML config for TimeScaleDB connection settings."""
    with path.open("r") as f:
        return yaml.safe_load(f)

CONFIG_PATH = Path(__file__).with_name("config.yaml")
cfg = load_config(CONFIG_PATH)

# Match pool size to the maximum concurrent requests the server can execute
GRPC_MAX_WORKERS = 10

# Pass connection limits directly into the reader configuration
reader = TimeScaleReader(cfg["timescaledb"], min_conn=5, max_conn=GRPC_MAX_WORKERS + 2)

STREAM_BATCH_SIZE = 1000
STREAM_POLL_SECONDS = 1.0

def row_to_datapoint(row):
    """Convert a DB row into a DWS Datapoint message.

    Chooses the correct oneof field (float/string/json) based on which
    value column is populated.
    """
    t, topic, component, metric, vnum, vtext, vjson = row
    dp = models_pb2.Datapoint()
    dp.topic = topic

    ts = Timestamp()
    ts.FromDatetime(t)
    dp.timestamp.CopyFrom(ts)

    if vnum is not None:
        dp.float_value = float(vnum)
    elif vtext is not None:
        dp.string_value = vtext
    elif vjson is not None:
        s = Struct()
        try:
            s.update(json.loads(vjson))
        except Exception:
            s.update({"value": vjson})
        dp.json_value.CopyFrom(s)

    return dp

class DataService(service_pb2_grpc.DataServiceServicer):
    def GetDataPoint(self, request, context):
        """Return the closest datapoint at/around the requested timestamp."""
        do_closest_past = getattr(request, "do_closest_past", True)
        row = reader.get_point(
            request.topic,
            request.timestamp.ToDatetime(),
            do_closest_past,
        )
        if not row:
            return service_pb2.GetDataPointResponse()
        return service_pb2.GetDataPointResponse(datapoint=row_to_datapoint(row))

    def GetDataRange(self, request, context):
        """Return a page of datapoints within the requested time range."""
        rows = reader.get_range(
            request.topic,
            request.start_time.ToDatetime(),
            request.end_time.ToDatetime(),
            request.page_size if request.page_size else 1000,
            request.page_token if request.page_token else None,
        )
        datapoints = [row_to_datapoint(r) for r in rows]
        next_token = rows[-1][0].isoformat() if rows else ""
        return service_pb2.GetDataRangeResponse(
            datapoints=datapoints,
            next_page_token=next_token
        )

    def StreamData(self, request, context):
        """Stream datapoints by polling the DB for new rows.

        This is a simple tailing loop: query for rows after `last_ts`, emit them,
        and sleep briefly when no new rows are available.
        """
        if request.HasField("start_from"):
            last_ts = request.start_from.ToDatetime()
        else:
            last_ts = datetime.now(timezone.utc)

        while context.is_active():
            rows = reader.stream_batch(request.topic, last_ts, STREAM_BATCH_SIZE)
            if not rows:
                # Back off briefly when no new rows are available.
                time.sleep(STREAM_POLL_SECONDS)
                continue

            for row in rows:
                last_ts = row[0]
                yield service_pb2.StreamDataResponse(datapoint=row_to_datapoint(row))

def serve():
    # Start gRPC server and register the DWS service.
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=GRPC_MAX_WORKERS))
    service_pb2_grpc.add_DataServiceServicer_to_server(DataService(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("TimeScale DWS server listening on 50051")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()