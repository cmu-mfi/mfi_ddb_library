from concurrent import futures
import logging

import grpc
import yaml
import stubs.models_pb2 as models_pb2
import stubs.service_pb2 as service_pb2
import stubs.service_pb2_grpc as service_pb2_grpc
import stubs.models_pb2_grpc as models_pb2_grpc

from piwebapi import PIWebAPI

with open('secrets.yaml', 'r') as file:
    secrets = yaml.safe_load(file)
    
pi_client = PIWebAPI(secrets)

class DataService(service_pb2_grpc.DataServiceServicer):
    def GetDataPoint(self, request, context):
        value = pi_client.get_data_point(request.topic, request.user_id, request.timestamp)
        return service_pb2.GetDataPointResponse(datapoint=value)
        
    def GetDataRange(self, request, context):
        values = pi_client.get_data_range(request.topic, request.user_id, request.start_time, request.end_time, request.page_size, request.page_token)
        return service_pb2.GetDataRangeResponse(datapoints=values)
        
    def StreamData(self, request, context):
        raise NotImplementedError("Streaming not implemented yet")
        for value in pi_client.stream_data(request.topic, request.user_id, request.start_from):
            yield service_pb2.StreamDataResponse(datapoint=value)

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