#!/bin/bash

ports=(1883 8083 18083 5432 5431 5430 50051 50052 50053 50054 8000 8001 3001 9000 9443)

in_use=()

for port in "${ports[@]}"; do
    if lsof -nP -iTCP:$port -sTCP:LISTEN >/dev/null; then
        in_use+=("$port")
    fi
done

if [ ${#in_use[@]} -eq 0 ]; then
    echo "ALL OK!"
else
    echo "Ports in use:"
    for port in "${in_use[@]}"; do
        echo "Port $port"
    done
fi