# PostgreSQL Key-Value Store Database Node

This directory contains the PostgreSQL-based key-value store database node for the MFI-DDB framework.

## Overview

This database node stores MQTT data in a PostgreSQL database with three columns:
- `timestamp`: The time when the MQTT message was received (TIMESTAMPTZ)
- `topic`: The MQTT topic the message was published to (TEXT)
- `payload`: The JSON payload from the MQTT message (JSONB)

## Initial Setup

### 1. Install and setup PostgreSQL

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib

sudo systemctl start postgresql
sudo systemctl enable postgresql

# Create a database user. Remember the password you set
sudo -u postgres createuser -P mfi

# Create database
sudo -u postgres createdb -O mfi mfi_kv

# Verify database setup
psql -U mfi -d mfi_kv -h localhost
```

### 2. Initialize Database Schema

Run the initialization script to create the kv_data table:

```bash
cd kv-psql/dws
python3 init_db.py --host localhost --port 5432 --database mfi_kv --user mfi --password your_password
```

### 3. Configure config.yaml

Edit `config.yaml` file in the `dws` and `connector` directory.

### 4. Run Tests (Optional)

To run the test suite:

```bash
cd kv-psql
pytest tests/ -v
```

For a fresh test database, you can use Docker:

```bash
docker run --rm -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=test_mfi_kv -p 5433:5432 postgres:14
```

Then run tests with:
```bash
pytest tests/ -v --tb=short
```

## Directory Structure

```
kv-psql/
├── connector/              # MQTT connector components
│   └── connector.py        # MQTT to database connector
├── dws/                    # Database Web Service components
│   ├── proto/              # Protobuf definitions
│   │   ├── models.proto
│   │   └── service.proto
│   ├── gen/                # Generated protobuf code
│   │   ├── models_pb2.py
│   │   ├── models_pb2_grpc.py
│   │   ├── service_pb2.py
│   │   └── service_pb2_grpc.py
│   ├── server.py           # gRPC server implementation
│   └── init_db.py          # Database initialization script
└── tests/                  # Test files
    └── test_dws.py         # DWS server tests
```

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
# or
uv sync
```

2. Generate protobuf code (if proto files were modified):
```bash
cd dws
bash build_all.sh
```

## Usage

### Starting the DWS Server

```bash
python dws/server.py
```

The server will read configuration from `dws/config.yaml`.

### Starting the Connector

```bash
python connector/connector.py
```

The connector will read configuration from `connector/config.yaml`.

## Database Schema

The database uses a single table `kv_data` with the following structure:

```sql
CREATE TABLE kv_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    topic TEXT NOT NULL,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Indexes

- `idx_kv_data_timestamp`: For efficient timestamp-based queries
- `idx_kv_data_topic`: For efficient topic-based queries
- `idx_kv_data_topic_timestamp`: For efficient combined topic and timestamp queries
- `idx_kv_data_created_at`: For efficient created_at queries

## Running Tests

### Pytest with Test Database

To run tests with a temporary test database:

```bash
pytest tests/ -v
```

Tests will automatically create and clean up a test database.