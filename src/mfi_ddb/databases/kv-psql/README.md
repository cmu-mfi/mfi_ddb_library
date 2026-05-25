# PostgreSQL Key-Value Store Database Node

This directory contains the PostgreSQL-based key-value store database node for the MFI-DDB framework.

## Overview

This database node stores MQTT data in a PostgreSQL database with three columns:
- `timestamp`: The time when the MQTT message was received (TIMESTAMPTZ)
- `topic`: The MQTT topic the message was published to (TEXT)
- `payload`: The JSON payload from the MQTT message (JSONB)

## Initial Setup

### 1. Install PostgreSQL

**Ubuntu/Debian:**
```bash
sudo apt update
sudo apt install postgresql postgresql-contrib
```

**macOS (Homebrew):**
```bash
brew install postgresql
brew services start postgresql
```

**Windows:**
Download and install from [postgresql.org](https://www.postgresql.org/download/windows/)

### 2. Start PostgreSQL Service

**Ubuntu/Debian:**
```bash
sudo systemctl start postgresql
sudo systemctl enable postgresql
```

**macOS (Homebrew):**
```bash
brew services start postgresql
```

### 3. Create Database User

Switch to the postgres user and create a new user:

```bash
sudo -u postgres createuser -P mfi_ddb_user
```

Enter a password when prompted (remember this for later).

### 4. Create Database

Create the database and assign ownership to the new user:

```bash
sudo -u postgres createdb -O mfi_ddb_user mfi_ddb
```

### 5. Verify Database Setup

Test the connection with psql:

```bash
psql -U mfi_ddb_user -d mfi_ddb -h localhost
```

You'll be prompted for the password you set. If successful, you'll see the psql prompt.

### 6. Initialize Database Schema

Run the initialization script to create the kv_data table:

```bash
cd kv-psql/dws
python3 init_db.py --host localhost --port 5432 --database mfi_ddb --user mfi_ddb_user --password your_password
```

### 7. Configure secrets.yaml

Create a `secrets.yaml` file in the kv-psql directory:

```yaml
postgres:
  host: localhost
  port: 5432
  database: mfi_ddb
  user: mfi_ddb_user
  password: your_password

mqtt:
  broker: localhost
  port: 1883
  client_id: kv-psql-connector
  topics:
    - "#"

dws:
  port: 50051
```

### 8. Run Tests (Optional)

To run the test suite:

```bash
cd kv-psql
pytest tests/ -v
```

For a fresh test database, you can use Docker:

```bash
docker run --rm -e POSTGRES_PASSWORD=testpass -e POSTGRES_DB=test_mfi_ddb -p 5433:5432 postgres:14
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

## Prerequisites

- Python 3.8+
- PostgreSQL 12+
- protobuf (>= 5.29.0)
- grpcio (>= 1.56.0)
- grpcio-tools (>= 1.56.0)
- paho-mqtt (>= 1.6.1)
- psycopg2-binary (>= 2.9.0)
- PyYAML (>= 6.0)

## Installation

1. Install dependencies:
```bash
pip install -r requirements.txt
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

The server will read configuration from `secrets.yaml` if present, or use default values.

### Starting the Connector

```bash
python connector/connector.py
```

The connector will read configuration from `secrets.yaml` if present.

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

## Configuration

Configuration is read from `secrets.yaml` in the kv-psql directory. A template is provided:

```yaml
postgres:
  host: localhost
  port: 5432
  database: mfi_ddb
  user: postgres
  password: your_password

mqtt:
  broker: localhost
  port: 1883
  client_id: kv-psql-connector
  topics:
    - "#"

dws:
  port: 50051
```

## MFI-DDB Compatibility

This database node is compatible with MFI-DDB Schema V1.0 key-value payloads.

## Running Tests

### Unit Tests

```bash
python -m tests.test_dws
```

### Pytest with Test Database

To run tests with a temporary test database:

```bash
pytest tests/ -v
```

Tests will automatically create and clean up a test database.