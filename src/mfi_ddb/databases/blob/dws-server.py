import logging
from concurrent import futures

import grpc
import stubs.models_pb2 as models_pb2
import stubs.models_pb2_grpc as models_pb2_grpc
import stubs.service_pb2 as service_pb2
import stubs.service_pb2_grpc as service_pb2_grpc
from google.protobuf.timestamp_pb2 import Timestamp
import yaml
from error_codes import GrpcError
from mfi_ddb.databases.blob.blobapi import BlobAPI

with open("config.yaml", "r") as file:
    config = yaml.safe_load(file)

cfg = config.get("config")
blob_api = BlobAPI(
    blob_dir=cfg.get("blob_dir"),
    index_path=cfg.get("index_path")
)


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

    def GetDataPoint(self, request, context):
        try:
            blob = self.blob_api.get_data_point(
                request.topic,
                request.user_id,
                request.timestamp
            )

            ts = Timestamp()
            ts.FromJsonString(blob.timestamp)

            datapoint = models_pb2.Datapoint(
                topic=blob.topic,
                timestamp=ts,
                file_value=blob.file
            )
        except Exception as e:
            self.__handle_exception(e, context)
            return service_pb2.GetDataPointResponse()

        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataPointResponse(datapoint=datapoint)

    def GetDataRange(self, request, context):
        try:
            values = self.blob_api.get_data_range(
                request.topic,
                request.user_id,
                request.start_time,
                request.end_time,
                request.page_size,
                request.page_token
            )
            
            if "data" not in values:
                self.logger.warning(f"No 'data' key in response for topic={request.topic}, user_id={request.user_id}")
            
            if "nextPageToken" not in values:
                self.logger.warning(f"No 'nextPageToken' key in response for topic={request.topic}, user_id={request.user_id}")

            datapoints = []
            for blob in values.get("data", []):
                ts = Timestamp()
                ts.FromJsonString(blob.timestamp)
                datapoint = models_pb2.Datapoint(
                    topic=blob.topic,
                    timestamp=ts,
                    file_value=blob.file
                )
                datapoints.append(datapoint)

        except Exception as e:
            self.__handle_exception(e, context)
            return service_pb2.GetDataRangeResponse()
        
        self.logger.info(f"GetDataRange success: topic={request.topic}, user_id={request.user_id}, datapoints={len(datapoints)}")
        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataRangeResponse(
            datapoints=datapoints,
            next_page_token=values.get("nextPageToken", "")
        )
    def StreamData(self, request, context):
            context.set_code(grpc.StatusCode.UNIMPLEMENTED)
            context.set_details("Streaming is not implemented yet.")
            return service_pb2.StreamDataResponse()
            # for value in pi_client.stream_data(request.topic, request.user_id, request.start_from):
            #     yield service_pb2.StreamDataResponse(datapoint=value)


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
