import os
import sys
import time
from datetime import datetime, timezone
import logging
from concurrent import futures
import grpc
import yaml
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'gen'))
sys.path.insert(0, os.path.dirname(__file__))
from error_codes import GrpcError
from piwebapi import PIWebAPI
from gen import models_pb2, service_pb2, service_pb2_grpc

with open("./config.yaml", "r") as file:
    secrets = yaml.safe_load(file)

pi_client = PIWebAPI(secrets)

from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


# Create meter and metrics
meter = get_meter("mfi.aveva_pi.dws")

grpc_requests_counter = meter.create_counter(
    "mfi_grpc_requests_total",
    description="Total number of gRPC requests handled by AVEVA PI DWS"
)

grpc_request_duration = meter.create_histogram(
    "mfi_grpc_request_duration_seconds",
    description="Time spent processing gRPC read requests in seconds"
)


class DataService(service_pb2_grpc.DataServiceServicer):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def __handle_exception(self, e, context):

        if type(e) in GrpcError.__subclasses__():
            status_code = getattr(
                grpc.StatusCode, e.status_code, grpc.StatusCode.UNKNOWN
            )
        else:
            status_code = grpc.StatusCode.UNKNOWN

        self.logger.error(f"PI DWS Error: {e}")
        context.set_details(str(e))
        context.set_code(status_code)

    def _timestamp_to_string(self, ts):
        dt = ts.ToDatetime().astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    def _datapoint_to_proto(self, item):
        dt = datetime.fromisoformat(item["timestamp"].replace("Z", "+00:00"))
        ts = {"seconds": int(dt.timestamp())}

        value = item["value"]
        if isinstance(value, int):
            return models_pb2.Datapoint(topic=item["topic"], timestamp=ts, int_value=value)
        elif isinstance(value, float):
            return models_pb2.Datapoint(topic=item["topic"], timestamp=ts, float_value=value)
        elif isinstance(value, str):
            return models_pb2.Datapoint(topic=item["topic"], timestamp=ts, string_value=value)
        elif isinstance(value, dict):
            return models_pb2.Datapoint(topic=item["topic"], timestamp=ts, json_value=value)
        else:
            return models_pb2.Datapoint(topic=item["topic"], timestamp=ts, string_value=str(value))
        
    def GetDataPoint(self, request, context):
        start_time = time.perf_counter()  # <-- 3. Start timer
        try:
            item = pi_client.get_data_point(
                request.topic,
                self._timestamp_to_string(request.timestamp),
                request.do_closest_past
            )
        except Exception as e:
            # Record failed request metrics
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "ERROR"})
            
            self.__handle_exception(e, context)
            return service_pb2.GetDataPointResponse()

        duration = time.perf_counter() - start_time  # <-- 4. Stop timer

        if item is None:
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "NOT_FOUND"})
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("No datapoint found")
            return service_pb2.GetDataPointResponse()

        # Record successful request metrics
        grpc_request_duration.record(duration, {"method": "GetDataPoint"})
        grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "OK"})
        
        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataPointResponse(datapoint=self._datapoint_to_proto(item))

    def GetDataRange(self, request, context):
        start_time = time.perf_counter()  # <-- 5. Start timer
        try:
            values = pi_client.get_data_range(
                request.topic,
                self._timestamp_to_string(request.start_time),
                self._timestamp_to_string(request.end_time),
                request.page_size,
                request.page_token,
            )
        except Exception as e:
            # Record failed request metrics
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataRange"})
            grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "ERROR"})
            
            self.__handle_exception(e, context)
            return service_pb2.GetDataRangeResponse()

        duration = time.perf_counter() - start_time  # <-- 6. Stop timer
        
        # Record successful request metrics
        grpc_request_duration.record(duration, {"method": "GetDataRange"})
        grpc_requests_counter.add(1, {"method": "GetDataRange", "status": "OK"})

        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataRangeResponse(
            datapoints=[self._datapoint_to_proto(item) for item in values["data"]],
            next_page_token=values["nextPageToken"] or ""
        )
    
    def StreamData(self, request, context):
        # Tracking unimplemented attempts
        grpc_requests_counter.add(1, {"method": "StreamData", "status": "UNIMPLEMENTED"})
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Streaming is not implemented yet.")
        return service_pb2.StreamDataResponse()


def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_DataServiceServicer_to_server(DataService(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
