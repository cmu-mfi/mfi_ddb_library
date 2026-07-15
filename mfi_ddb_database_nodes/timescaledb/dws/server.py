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

from db import TimeScaleReader
from gen import models_pb2, service_pb2, service_pb2_grpc


from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter  # <-- 1. Added get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader_metric = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader_metric])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
meter = get_meter("mfi.timescaledb.dws")

grpc_requests_counter = meter.create_counter(
    "mfi_timescale_dws_grpc_requests_total",
    description="Total gRPC requests processed by Timescale DWS"
)

grpc_request_duration = meter.create_histogram(
    "mfi_timescale_dws_grpc_duration_seconds",
    description="Duration of gRPC request processing in seconds"
)

returned_rows_histogram = meter.create_histogram(
    "mfi_timescale_dws_returned_rows",
    description="Number of database rows returned per retrieval request"
)
# ==========================================


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
        """Return a datapoint from the requested side of the timestamp.

        The caller chooses the side with do_closest_past:
        - True: return the newest row at or before the requested timestamp.
        - False: return the oldest row at or after the requested timestamp.
        """
        start_time = time.perf_counter()  # <-- 2. Start timer
        try:
            do_closest_past = getattr(request, "do_closest_past", True)
            row = reader.get_point(
                request.topic,
                request.timestamp.ToDatetime(),
                do_closest_past,
            )
            
            duration = time.perf_counter() - start_time  # <-- 3. Stop timer
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})

            if not row:
                grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "NOT_FOUND"})
                return service_pb2.GetDataPointResponse()
            
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "OK"})
            returned_rows_histogram.record(1, {"method": "GetDataPoint"})
            return service_pb2.GetDataPointResponse(datapoint=row_to_datapoint(row))

        except Exception as e:
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "ERROR"})
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return service_pb2.GetDataPointResponse()

    def GetDataRange(self, request, context):
        """Return a page of datapoints within the requested time range.

        The request topic can be an exact topic or a wildcard filter. The reader
        handles the topic matching and keeps the existing time-based cursor
        contract for pagination.
        """
        start_time = time.perf_counter()  # <-- 4. Start timer
        try:
            rows = reader.get_range(
                request.topic,
                request.start_time.ToDatetime(),
                request.end_time.ToDatetime(),
                request.page_size if request.page_size else 1000,
                request.page_token if request.page_token else None,
            )
            
            datapoints = [row_to_datapoint(r) for r in rows]
            next_token = rows[-1][0].isoformat() if rows else ""
            
            duration = time.perf_counter() - start_time  # <-- 5. Stop timer
            grpc_request_duration.record(duration, {"method": "GetDataRange"})
            grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "OK"})
            returned_rows_histogram.record(len(datapoints), {"method": "GetDataRange"})

            return service_pb2.GetDataRangeResponse(
                datapoints=datapoints,
                next_page_token=next_token
            )
            
        except Exception as e:
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataRange"})
            grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "ERROR"})
            
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Database error: {str(e)}")
            return service_pb2.GetDataRangeResponse()

    def StreamData(self, request, context):
        """Stream datapoints by polling the DB for new rows.

        This is a simple tailing loop: query for rows after `last_ts`, emit them,
        and sleep briefly when no new rows are available. The topic filter is
        forwarded as-is, which lets the caller tail a single topic or a wildcard
        family of topics.
        """
        if request.HasField("start_from"):
            last_ts = request.start_from.ToDatetime()
        else:
            last_ts = datetime.now(timezone.utc)

        grpc_requests_counter.add(1, {"method": "StreamData", "status": "OK"})

        try:
            while context.is_active():
                rows = reader.stream_batch(request.topic, last_ts, STREAM_BATCH_SIZE)
                if not rows:
                    # Back off briefly when no new rows are available.
                    time.sleep(STREAM_POLL_SECONDS)
                    continue

                # Record size of the chunks being pushed down the stream
                returned_rows_histogram.record(len(rows), {"method": "StreamData"})

                for row in rows:
                    last_ts = row[0]
                    yield service_pb2.StreamDataResponse(datapoint=row_to_datapoint(row))
                    
        except Exception as e:
            grpc_requests_counter.add(1, {"method": "StreamData", "status": "ERROR"})
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"Stream error: {str(e)}")

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