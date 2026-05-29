#!/bin/bash

protoc="python3 -m grpc_tools.protoc"
protopath="../dws/proto"
output="./stubs"

find $protopath -type f -print0 | while IFS= read -r -d $'\0' file; do
    $protoc -I $protopath --python_out=$output --pyi_out=$output --grpc_python_out=$output $file
done