# Aveva PI Store Database Node

This directory contains the Aveva PI - Time Series store database node for the MFI-DDB framework.

## Overview

This database node stores MQTT data in a Aveva PI database:
* It uses a [PI Connector for MQTT](https://docs.aveva.com/bundle/pi-connector-for-mqtt-sparkplug/page/1010383.html) to subscribe to the topic and then translates that into a tag and data value.
* Steps below indicate how to get a DWS server running to allow MFI-DDB retrieval api to get stored MQTT data.

## Prerequisites
* [Aveva PI Data Archive](https://docs.aveva.com/bundle/pi-interface-for-performance-monitor/page/1009343.html)
* [Aveva PI Web API](https://docs.aveva.com/bundle/pi-web-api-reference/page/help.html)
* [Connector for MQTT Sparkplug 1.0](https://docs.aveva.com/bundle/pi-connector-for-mqtt-sparkplug/page/1010383.html)

## Initial Setup - DWS

### 1. Configure config.yaml
Edit `config.yaml` file in the `dws` directory.

### 2. Verify Connectivity
Ensure the configured PI Web API endpoint is reachable and that the configured user has permission to read PI tags.

## Directory Structure

```
aveva-pi/
├── dws/                    # Database Web Service components
│   ├── proto/              # Protobuf definitions
│   │   ├── models.proto
│   │   └── service.proto
│   ├── gen/                # Generated protobuf code
│   │   ├── models_pb2.py
│   │   ├── models_pb2_grpc.py
│   │   ├── service_pb2.py
│   │   └── service_pb2_grpc.py
│   └── server.py           # gRPC server implementation
└── tests/                  # Test files
    ├── test_piwebapi.py         # PI tests
    └── test_server.py           # gRPC server tests
```

## Usage - DWS

### Starting the DWS Server

```bash
python dws/server.py
```

The server will read configuration from `dws/config.yaml` and exposes a gRPC endpoint on port: 50051

## Running Tests

### Pytest with Test Database

To run tests:

```bash
pytest tests/ -v
```

This will run two sets of tests:
* `test_piwebapi` - testing the functunality of PI WEB API
* `test_server` - testing the gRPC server
