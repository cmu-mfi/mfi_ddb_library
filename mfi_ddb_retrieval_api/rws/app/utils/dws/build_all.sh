# #!/bin/bash

# protoc="python3 -m grpc_tools.protoc"
# protopath="./proto"
# output="./gen"

# find $protopath -type f -print0 | while IFS= read -r -d $'\0' file; do
#     $protoc -I $protopath --python_out=$output --pyi_out=$output --grpc_python_out=$output $file
# done

#!/bin/bash
# Build script for generating package-aware gRPC Python code inside /gen

set -e

# 1. Determine local paths relative to this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROTO_DIR="$SCRIPT_DIR/proto"
GEN_DIR="$SCRIPT_DIR/gen"

# Ensure gen directory exists and is clean
mkdir -p "$GEN_DIR"

echo "Generating gRPC code directly into /gen..."

# 2. Run protoc localized to your proto directory
python3 -m grpc_tools.protoc \
    -I"$PROTO_DIR" \
    --python_out="$GEN_DIR" \
    --pyi_out="$GEN_DIR" \
    --grpc_python_out="$GEN_DIR" \
    "$PROTO_DIR/models.proto" \
    "$PROTO_DIR/service.proto"

echo "Fixing internal cross-imports for application layout..."

# 3. Patch BOTH models_pb2 and service_pb2 imports into clean relative imports
if [[ "$OSTYPE" == "darwin"* ]]; then
    # macOS syntax
    sed -i '' 's/import models_pb2 as/from . import models_pb2 as/g' "$GEN_DIR"/*_pb2*.py
    sed -i '' 's/import service_pb2 as/from . import service_pb2 as/g' "$GEN_DIR"/*_pb2*.py
else
    # Linux / Docker syntax
    sed -i 's/import models_pb2 as/from . import models_pb2 as/g' "$GEN_DIR"/*_pb2*.py
    sed -i 's/import service_pb2 as/from . import service_pb2 as/g' "$GEN_DIR"/*_pb2*.py
fi

echo "Generation complete! Files neatly placed and patched in $GEN_DIR"