#!/bin/bash
# Build script for generating gRPC Python code from .proto files

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/proto"
GEN_DIR="$SCRIPT_DIR/gen"

# Check if protoc is available
if ! command -v python3 -m grpc_tools.protoc &> /dev/null; then
    echo "Error: grpc_tools.protoc not found. Install with: pip install grpcio-tools"
    exit 1
fi

# Ensure gen directory exists
mkdir -p "$GEN_DIR"

echo "Generating gRPC code from .proto files..."

# Compile proto files (models.proto must be compiled first due to dependencies)
python3 -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$GEN_DIR" \
    --pyi_out="$GEN_DIR" \
    --grpc_python_out="$GEN_DIR" \
    "$PROTO_DIR/models.proto" \
    "$PROTO_DIR/service.proto"

echo "Generation complete. Files created in $GEN_DIR:"
ls -la "$GEN_DIR"/*.py

echo ""
echo "Done!"
