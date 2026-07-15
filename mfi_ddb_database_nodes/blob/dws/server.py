import logging
from concurrent import futures
import os
import time  # <-- 1. Added time import for timing operations
# from time import timezone. #this is only an integer does not have the utc attribute
from datetime import timezone

import grpc
import gen.models_pb2 as models_pb2
import gen.models_pb2_grpc as models_pb2_grpc
import gen.service_pb2 as service_pb2
import gen.service_pb2_grpc as service_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp
import yaml
# from mfi_ddb_library.src.mfi_ddb.databases.blob.dws.error_codes import GrpcError
# from mfi_ddb_library.src.mfi_ddb.databases.blob.dws.blobapi import BlobAPI
from error_codes import GrpcError
from blobapi import BlobAPI

CONFIG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.yaml")
with open(CONFIG_PATH, "r") as file:
    config = yaml.safe_load(file)

cfg = config.get("config")
blob_api = BlobAPI(
    blob_dir=cfg.get("blob_dir"),
    index_path=cfg.get("index_path")
)


from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter  # <-- 2. Added get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")


# ==========================================
# OPENTELEMETRY CUSTOM METRICS SETUP
# ==========================================
meter = get_meter("mfi.blob.dws")

grpc_requests_counter = meter.create_counter(
    "mfi_blob_dws_grpc_requests_total",
    description="Total gRPC requests processed by Blob DWS"
)

grpc_request_duration = meter.create_histogram(
    "mfi_blob_dws_grpc_duration_seconds",
    description="Duration of gRPC request processing in seconds"
)

served_bytes_histogram = meter.create_histogram(
    "mfi_blob_dws_served_bytes",
    description="Size of files (blobs) served to clients in bytes"
)
# ==========================================


class DataService(service_pb2_grpc.DataServiceServicer):
    def __init__(self, blob_api):
        self.blob_api = blob_api
        self.logger = logging.getLogger(__name__)

    def __handle_exception(self, e, context):
        if type(e) in GrpcError.__subclasses__():
            status_code = getattr(
            grpc.StatusCode, e.status_code, grpc.StatusCode.UNKNOWN)
        else:
            status_code = grpc.StatusCode.UNKNOWN

        self.logger.error(f"Local Files DWS Error: {e}")
        context.set_details(str(e))
        context.set_code(status_code)

    def _timestamp_to_string(self, ts):
        dt = ts.ToDatetime().astimezone(timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")

    def GetDataPoint(self, request, context):
        start_time = time.perf_counter()  # <-- 3. Start timer
        try:
            blob = self.blob_api.get_data_point(
                request.topic,
                self._timestamp_to_string(request.timestamp),
                request.do_closest_past
            )

            ts = Timestamp()
            ts.FromJsonString(blob.timestamp)

            datapoint = models_pb2.Datapoint(
                topic=blob.topic,
                timestamp=ts,
                file_value=blob.file
            )
        except Exception as e:
            # Record failed request metrics
            duration = time.perf_counter() - start_time
            grpc_request_duration.record(duration, {"method": "GetDataPoint"})
            grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "ERROR"})
            
            self.__handle_exception(e, context)
            return service_pb2.GetDataPointResponse()

        duration = time.perf_counter() - start_time  # <-- 4. Stop timer
        
        # Record successful request metrics
        grpc_request_duration.record(duration, {"method": "GetDataPoint"})
        grpc_requests_counter.add(1, {"method": "GetDataPoint", "status": "OK"})
        
        # Record file size served (if file exists)
        if blob and blob.file:
            served_bytes_histogram.record(len(blob.file), {"method": "GetDataPoint"})

        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataPointResponse(datapoint=datapoint)

    def GetDataRange(self, request, context):
        start_time = time.perf_counter()  # <-- 5. Start timer
        try:
            values = self.blob_api.get_data_range(
                request.topic,
                self._timestamp_to_string(request.start_time), 
                self._timestamp_to_string(request.end_time),
                request.page_size,
                request.page_token
            )
            
            if "data" not in values:
                self.logger.warning(f"No 'data' key in response for topic={request.topic}, user_id={request.user_id}")
            
            if "nextPageToken" not in values:
                self.logger.warning(f"No 'nextPageToken' key in response for topic={request.topic}, user_id={request.user_id}")

            datapoints = []
            total_bytes_served = 0
            for blob in values.get("data", []):
                ts = Timestamp()
                ts.FromJsonString(blob.timestamp)
                datapoint = models_pb2.Datapoint(
                    topic=blob.topic,
                    timestamp=ts,
                    file_value=blob.file
                )
                datapoints.append(datapoint)
                
                # Accumulate size of all blobs in the range
                if blob.file:
                    total_bytes_served += len(blob.file)

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
        
        if total_bytes_served > 0:
            served_bytes_histogram.record(total_bytes_served, {"method": "GetDataRange"})
        
        self.logger.info(f"GetDataRange success: topic={request.topic}, user_id={request.user_id}, datapoints={len(datapoints)}")
        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataRangeResponse(
            datapoints=datapoints,
            next_page_token=values.get("nextPageToken", "")
        )

    def StreamData(self, request, context):
        grpc_requests_counter.add(1, {"method": "StreamData", "status": "UNIMPLEMENTED"})
        context.set_code(grpc.StatusCode.UNIMPLEMENTED)
        context.set_details("Streaming is not implemented yet.")
        return service_pb2.StreamDataResponse()

def serve():
    port = "50051"
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    service_pb2_grpc.add_DataServiceServicer_to_server(DataService(blob_api), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()