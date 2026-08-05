# MFI DDB Library - Docker Setup

This directory contains Docker Compose configurations for the MFI Data-Driven Building (DDB) Library pipeline.

## Overview

The Docker pipeline implements the MFI DDB architecture with MQTT as the central pub-sub broker:

```mermaid
flowchart BT
    subgraph Input["."]
        direction LR

        subgraph DA_App["Data Adapter App"]
            DAA_Backend[data-adapter-backend]
            DAA_Frontend[data-adapter-frontend]
        end

        subgraph Data_Sources["Data Sources"]
            direction LR
            DS1[DataAdapter 1] --> S1[Streamer 1]
            DS2[DataAdapter ...] --> S2[Streamer 2]
            DS3[DataAdapter ...] --> S3[Streamer 3]
        end
    end

    MQTT[Pub-Sub Broker: EMQX]

    subgraph DBN["Database Nodes"]
        direction LR
        KV[KV-PSQL Node]
        BLOB[Blob Storage Node]
        HIST[TimescaleDB Node]
    end

    METADATA[MDS + RWS Node]

    S1 --> MQTT
    S2 --> MQTT
    S3 --> MQTT
    MQTT --> KV
    MQTT --> BLOB
    MQTT --> HIST
    MQTT --> METADATA
    METADATA --> USERS[Users/Applications]
    DBN --> METADATA

    classDef layer fill:#e6f7ff,stroke:#1890ff
    classDef highlight fill:#094d57,stroke:#0a3d4d
    class Input,DBN,METADATA layer
```

> [!Note]
> Arrow direction shows data flow in the framework. For service ports and descriptions, see the Services section below.

## Services

### Shared Infrastructure

| Service | Port(s) | Description | Profile |
|---------|---------|-------------|---------|
| `mqtt-broker` | 1883, 8083, 18083 | EMQX MQTT broker with WebSocket and dashboard | `mqtt-broker` |

### Database Nodes

Each database node consists of three services: a database, a connector (MQTT-to-DB), and a DWS gRPC service.

| Node | Database Port | DWS Port | Description |
|------|---------------|----------|-------------|
| **KV-PSQL** | 5431 | 50051 | Key-value PostgreSQL (mfi_kv) with connector + DWS |
| **TimescaleDB** | 5432 | 50052 | Time-series database (ddb_ts) with connector + DWS |
| **Metadata RWS** | 5430 | - | Metadata store (mds) with connector + RWS API |

#### KV-PSQL Node
| Service | Description | Profile |
|---------|-------------|---------|
| `kv-psql-db` | PostgreSQL database (port 5431) | `kv`, `dbn`|
| `kv-psql-connector` | Reads MQTT, writes to KV PostgreSQL | `kv`, `dbn`|
| `kv-psql-dws` | gRPC service (port 50051) | `kv`, `dbn`|

#### TimescaleDB Node
| Service | Description | Profile |
|---------|-------------|---------|
| `timescaledb-db` | PostgreSQL/TimescaleDB database (port 5432) | `ts`, `dbn` |
| `timescaledb-connector` | Reads MQTT, writes to TimescaleDB | `ts`, `dbn` |
| `timescaledb-dws` | gRPC service (port 50052) | `ts`, `dbn` |

#### Metadata RWS Node
| Service | Description | Profile |
|---------|-------------|---------|
| `metadata-store-db` | PostgreSQL metadata database (port 5430) | `retrieval` |
| `metadata-store-connector` | Reads MQTT, writes to metadata store | `retrieval` |
| `rws-app` | REST API service (port 8000) | `retrieval` |

#### Blob Storage Node
| Service | Description | Profile |
|---------|-------------|---------|
| `blob-connector` | Stores binary data from MQTT to blob storage | `blob`, `dbn` |
| `blob-dws` | gRPC service for blob access (port 50053) | `blob`, `dbn` |

#### Database-Specific Services
| Service | Port | Description | Profile |
|---------|------|-------------|---------|
| `aveva-pi-dws` | 50054 | Aveva PI integration via PI Web API gRPC service | `aveva`, `dbn` |

### Web Applications

| Service | Port | Description | Profile |
|---------|------|-------------|---------|
| `data-adapter-backend` | 8001 | Data adapter FastAPI backend | `daa` |
| `data-adapter-frontend` | 3001 | Data adapter frontend (Nginx) | `daa` |


### Development & Management Tools

| Service | Port(s) | Description | Profile |
|---------|---------|-------------|---------|
| `portainer` | 9000, 9443 | Portainer CE Docker management web UI | `dev-tools` |

## Quick Start

Start all services:
```bash
cd docker
docker compose --profile '*' up -d
```

Start a particular service. Can add multiple profile names of only one of them.
```bash
docker compose --profile <profile-name> <profile-name> up -d
```

View all logs (for specific, mention the profile you want to see the logs for, otherwise use `docker logs <container-name>`):
```bash
docker compose --profile '*' logs -f
```

Stop all services:
```bash
docker compose --profile '*' down
```

Stop and remove volumes:
```bash
docker compose --profile '*' down -v
```

## Configuration Files

Each service directory contains configuration files that need to be edited before use:

- `aveva/config.yaml` - Aveva PI connection settings
- `kv-psql/connector-config.yaml` - KV connector MQTT settings
- `kv-psql/dws-config.yaml` - KV DWS configuration
- `timescale/connector-config.yaml` - Timescale connector settings
- `timescale/dws-config.yaml` - Timescale DWS configuration
- `blob/connector-config.yaml` - Blob connector settings
- `blob/dws-config.yaml` - Blob DWS configuration
- `metadata-rws/*.ini`, `*.yaml` - Metadata store and RWS configurations

## Ports Reference

| Port | Service |
|------|---------|
| 1883 | MQTT broker (TCP) |
| 8083 | MQTT broker (WebSocket) |
| 18083 | EMQX dashboard UI |
| 5432 | TimescaleDB |
| 5431 | KV-PSQL database |
| 5430 | Metadata Store PostgreSQL |
| 50051 | KV DWS gRPC |
| 50052 | Timescale DWS gRPC |
| 50053 | Blob DWS gRPC |
| 50054 | Aveva PI DWS gRPC |
| 8000 | RWS API |
| 8001 | Data Adapter Backend |
| 3001 | Data Adapter Frontend |

## Volumes

Data is persisted in `.data/` directory:

- `.data/emqx/data` - EMQX data
- `.data/emqx/log` - EMQX logs
- `.data/kv_psql_storage` - KV PostgreSQL data
- `.data/timescale_storage` - TimescaleDB data
- `.data/mds_storage` - Metadata Store data
- `.data/blob_storage` - Blob storage

## Network

All services communicate on the `mfi_network` bridge network.