import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional

import grpc
from google.protobuf import json_format
from pydantic import BaseModel, Field

from mfi_ddb.data_adapters.base import BaseDataAdapter


class _SCHEMA(BaseModel):
    class _GRPCComponent(BaseModel):
        component_id: str = Field(..., description="ID of the gRPC component to monitor")
        attributes: Optional[dict] = Field(None, description="Additional attributes for the gRPC component (optional)")
        proto_path: str = Field(..., description="Path to the .proto file defining the gRPC service")
        request_method: str = Field(..., description="Name of the gRPC service method to call. Only `Read` method is supported.")
        stub_class: str = Field(..., description="Name of the gRPC stub class to use for communication")
        request_class: str = Field(..., description="Name of the request class to use for the gRPC service call.")
        request: dict = Field(..., description="Request parameters for the gRPC service call")
        
    class SCHEMA(BaseModel):
        server_address: str = Field(..., description="Network Address of the gRPC server")
        server_port: int = Field(..., description="Port of the gRPC server")
        certificate_path: str = Field("", description="Path to the gRPC server certificate file (optional)")
        protobufs_dir: str = Field(..., description="Path to the directory containing .proto files")
        compiled_protos_dir: str = Field("./compiled_protos", description="Path to the compiled protobuf stubs (optional)")
        trial_id: str = Field(..., description="Trial ID for the gRPC device. No spaces or special characters allowed.")
        components: List['_GRPCComponent'] = Field(..., description="List of gRPC components to monitor")
        
class GrpcDataAdapter(BaseDataAdapter):
    
    NAME = "gRPC"
    
    CONFIG_HELP = {
        "server_address": "Network Address of the gRPC server (string)",
        "server_port": "Port of the gRPC server (int)",
        "certificate_path": "Path to the gRPC server certificate file (optional, string)",
        "protobufs_dir": "Path to the directory containing .proto files (string)",
        "compiled_protos_dir": "Path to the compiled protobuf stubs (optional, string)",
        "trial_id": "Trial ID for the gRPC device. No spaces or special characters allowed. (string)",
        "components": "List of gRPC components to monitor (list of dicts with keys: component_id, attributes (optional), proto_path, stub_class, request_class, request)"
    }
    
    CONFIG_EXAMPLE = {
        "server_address": "localhost",
        "server_port": 50051,
        "certificate_path": "/path/to/cert.pem",
        "trial_id": "proj_zbc0505",
        "protobufs_dir": "/path/to/protos",
        "compiled_protos_dir": "/path/to/compiled_protos",
        "components": [
            {
                "component_id": "sensor.temperature",
                "attributes": {
                    "description": "something something",
                    "unit": "Celsius"
                },
                "proto_rel_path": "/path/to/temperature.proto",
                "stub_class": "TemperatureServiceStub",
                "request_class": "TemperatureRequest",
                "request": {
                    "key1": "value1",
                    "key2": [1, 2, 3]
                }
            },
            {
                "component_id": "sensor.humidity",
                "attributes": {
                    "description": "something something",
                    "unit": "Percentage"
                },
                "proto_rel_path": "/path/to/humidity.proto",
                "stub_class": "HumidityServiceStub",
                "request_class": "HumidityRequest",
                "request": {
                    "threshold": 75
                }
            }
        ]
    }
    
    RECOMMENDED_TOPIC_FAMILY = "historian"
    
    SELF_UPDATE = False
    
    class SCHEMA(BaseDataAdapter.SCHEMA, _SCHEMA.SCHEMA):
        """
        Schema for the MTConnect data adapter configuration.
        """
        pass

    def __init__(self, config: dict):
        super().__init__()
        
        self.cfg = config
        
        # 1. OPEN GRPC CHANNEL
        addr = f"{self.cfg['server_address']}:{self.cfg['server_port']}"
        if self.cfg['certificate_path']:
            with open(self.cfg['certificate_path'], 'rb') as f:
                root_certificates = f.read()
            cred = grpc.ssl_channel_credentials(root_certificates=root_certificates)
            self.channel = grpc.secure_channel(addr, cred)
        else:
            self.channel = grpc.insecure_channel(addr)
        try:
            grpc.channel_ready_future(self.channel).result(timeout=5)
        except grpc.FutureTimeoutError:
            raise ConnectionError('Error connecting to gRPC server')
        
        # 2. PREPARE COMPILED PROTOBUFS
        if self.cfg['compiled_protos_path']:
            if not os.path.exists(self.cfg['compiled_protos_path']):
                os.makedirs(self.cfg['compiled_protos_path'])

        compiled_dir = Path(self.cfg['compiled_protos_path'])

        self.did_i_compile_once = False
        self.components = []
        i = 0
        while i < len(self.cfg['components']):
            component = self.cfg['components'][i]
            proto_name = component['proto_rel_path'].split('/')[-1].replace('.proto', '')
            
            # Import compiled protobuf stubs dynamically
            # ``````````````````````````````````````````
            stub_spec = importlib.util.spec_from_file_location(proto_name,
                                                               os.path.join(compiled_dir, f"{proto_name}_pb2_grpc.py"))
            if not stub_spec or not stub_spec.loader:
                if not self.did_i_compile_once:
                    self.__generate_compiled_protos()
                    self.did_i_compile_once = True
                    continue
                raise ImportError(f"Could not load compiled protobuf stub for {proto_name}")
            stub_file = importlib.util.module_from_spec(stub_spec)
            
            stub_class = getattr(stub_file, component['stub_class'])
            
            # Import compiled protobuf request dynamically
            # ``````````````````````````````````````````            
            request_spec = importlib.util.spec_from_file_location(proto_name,
                                                                 os.path.join(compiled_dir, f"{proto_name}_pb2.py"))
            if not request_spec or not request_spec.loader:
                if not self.did_i_compile_once:
                    self.__generate_compiled_protos()
                    self.did_i_compile_once = True
                    continue
                raise ImportError(f"Could not load compiled protobuf request for {proto_name}")
            request_file = importlib.util.module_from_spec(request_spec)

            request_class = getattr(request_file, component['request_class'])
            request_dict = component['request']
            
            # Instantiate stub and request
            # ````````````````````````````            
            stub = stub_class(self.channel)
            request_instance = request_class(**request_dict)


            # Populate BaseDataAdapter data members: component_ids and attributes
            # ```````````````````````````````````````````````````````````````````
            attributes = component.get('attributes', {})
            self.component_ids.append(component['component_id'])
            self.attributes[component['component_id']] = attributes
            self._data[component['component_id']] = {}
            

            self.components.append({
                'component_id': component['component_id'],
                'stub': stub,
                'request_instance': request_instance,
                'request_method': component['request_method']
            })
            i = i + 1

    def get_data(self):
        for comp in self.components:
            response = getattr(comp['stub'], comp['request_method'])(comp['request_instance'])
            # Assuming response has a 'data' attribute; adjust as needed
            raw_response = response.data
            self._data[comp['component_id']] = self.__process_data(raw_response, comp['component_id'])
            self.last_updated[comp['component_id']] = time.time()
        
    def __generate_compiled_protos(self):
        '''
        #!/bin/bash

        protoc="python3 -m grpc_tools.protoc"
        protopath="./protobuf"
        output="./ServiceStubs"

        find $protopath -type f -print0 | while IFS= read -r -d $'\0' file; do
            $protoc -I $protopath --python_out=$output --grpc_python_out=$output $file
        done
        '''
        protoc = [sys.executable, "-m", "grpc_tools.protoc"]
        protopath = self.cfg.get('proto_base_path', './protobuf')
        output = self.cfg['compiled_protos_path']

        find_cmd = ["find", protopath, "-type", "f", "-print0"]
        while True:
            file = subprocess.run(find_cmd, capture_output=True, text=True)
            if not file.stdout:
                break
            file = file.stdout.strip('\0')
            subprocess.run(protoc + ["-I" + protopath, "--python_out=" + output, "--grpc_python_out=" + output, file], check=True)
            
    def __process_data(self, raw_response):
        '''
        Convert to strict keyed dict
        '''
        raw_data = json_format.MessageToDict(raw_response)
        processed_data = {}
        for key in raw_data.keys():
             processed_data.update(self.__extract_key_value(raw_data[key], key))

        return processed_data
    
    def __extract_key_value(self, data_item, data_item_key):
        if len(data_item_key) > 0 and data_item_key[0] == '/':
            data_item_key = data_item_key[1:]
        if isinstance(data_item, dict):
            extracted_data = {}
            for key in data_item.keys():
                substitute_key = key
                extracted_data.update(self.__extract_key_value(data_item[key], f'{data_item_key}/{substitute_key}'))
            return extracted_data
        elif isinstance(data_item, list):
            extracted_data = {}
            for i, item in enumerate(data_item):
                extracted_data.update(self.__extract_key_value(item, f'{data_item_key}_{i}'))
            return extracted_data
        else:
            return {data_item_key: self.__autotype(data_item)}
    
    def __autotype(self, value):
        for cast in (int, float, eval):
            try:
                if cast is eval:
                    return eval(value.replace("true", "True").replace("false", "False"))
                else:
                    if cast is int and '.' in value:
                        return float(value)
                    return cast(value)
            except:
                continue

        return value        