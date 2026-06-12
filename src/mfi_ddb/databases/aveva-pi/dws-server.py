import logging
from concurrent import futures

import grpc
import requests
import stubs.models_pb2 as models_pb2
import stubs.models_pb2_grpc as models_pb2_grpc
import stubs.service_pb2 as service_pb2
import stubs.service_pb2_grpc as service_pb2_grpc
import yaml
from error_codes import GrpcError
from piwebapi import PIWebAPI

with open("secrets.yaml", "r") as file:
    secrets = yaml.safe_load(file)

pi_client = PIWebAPI(secrets)


class DataService(service_pb2_grpc.DataServiceServicer):
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

    def GetDataPoint(self, request, context):
        self.logger = logging.getLogger(__name__)
        try:
            value = pi_client.get_data_point(
                request.topic, request.user_id, request.timestamp
            )
        except Exception as e:
            self.__handle_exception(e, context)
            return service_pb2.GetDataPointResponse()

        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataPointResponse(datapoint=value)

    def GetDataRange(self, request, context):
        try:
            values = pi_client.get_data_range(
                request.topic,
                request.user_id,
                request.start_time,
                request.end_time,
                request.page_size,
                request.page_token,
            )
        except Exception as e:
            self.__handle_exception(e, context)
            return service_pb2.GetDataRangeResponse()

        context.set_code(grpc.StatusCode.OK)
        return service_pb2.GetDataRangeResponse(
            datapoints=values["data"], next_page_token=values["nextPageToken"]
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
    service_pb2_grpc.add_DataServiceServicer_to_server(DataService(), server)
    server.add_insecure_port("[::]:" + port)
    server.start()
    print("Server started, listening on " + port)
    server.wait_for_termination()


if __name__ == "__main__":
    logging.basicConfig()
    serve()
