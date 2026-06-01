# Aveva PI Store Database Node

This directory contains the Aveva PI - Time Series store database node for the MFI-DDB framework.

## Overview

This database node stores MQTT data in a Aveva PI database:
It uses a data adapter by MQTT to subscribe to the topic and then translates that into a tag and data value

## Requirements
    Aveva PI Data Archive
    Aveva PI Web API
## Initial Setup

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
``` └── test_server.py           # gRPC server tests

## Usage

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
piwebapi-tests- testing the functunality of pi
grpc - testing the grpc server