# TimeScaleDB Database Node

This directory implements an open-source historian replacement for Aveva PI using TimeScaleDB. It follows the standard MFI-DDB database node pattern:

* **Connector:** Subscribes to MQTT historian topics, decodes incoming Sparkplug B payloads, and batches writes to TimeScaleDB.
* **Database:** TimeScaleDB (PostgreSQL enhanced with time-series extensions/hypertables).
* **DWS (Data Web Service):** A gRPC service that exposes APIs for high-performance time-series data retrieval (`GetDataPoint`, `GetDataRange`, and `StreamData` using an intelligent polling tail-loop).

## Folder Structure

```text
timescaledb/
│
├── connector/                      # THE INGESTION ENGINE (Write Path)
│   │                               # Manages real-time MQTT subscription and high-throughput ingestion.
│   │
│   ├── main.py                     # Pipeline Coordinator & Queue Worker
│   │                               └── Core Logic: 
│   │                                   • Establishes the Paho-MQTT client connection loop.
│   │                                   • Subscribes to the industrial network root topic (`mfi-v1.0-historian/#`).
│   │                                   • Intercepts raw Sparkplug B Protocol Buffer binary payloads.
│   │                                   • Extracts timestamps, maps metric names, and determines type classifications.
│   │                                   • Hands data over to an in-memory `queue.Queue` buffer with drop-safe boundaries.
│   │                                   • Spawns a background worker thread (`db_batch_writer_worker`) that runs a 
│   │                                     micro-batch loop, flushing items when `max_batch_size` 
│   │                                     or `flush_interval_sec` is reached to prevent write lockouts.
│   │
│   ├── db.py                       # TimeScaleDB Writer Client Layer
│   │                               └── Core Logic:
│   │                                   • Concrete implementation subclassing the core database writer interface.
│   │                                   • Implements the `insert_rows(rows)` method using Psycopg single-transaction blocks.
│   │                                   • Uses bulk execution arrays (`executemany`) targeting the Timescale hypertable.
│   │                                   • Handles connection drop-outs, transaction rollbacks, and reconnection retries.
│   │
│   ├── config.yaml                 # Connector Operational Profiles
│   │                               └── Details: Broking addresses, authentication credentials, batching limits 
│   │                                            (sizes/timeouts), and target PostgreSQL credentials.
│   │
│   └── requirements.txt            # Ingestion Dependencies (paho-mqtt, psycopg2-binary, sparkplug-b-protobuf).
│
├── dws/                            # THE DATA WEB SERVICE (Read Path)
│   │                               # Exposes the standard MFI-DDB gRPC query and streaming interfaces.
│   │
│   ├── server.py                   # gRPC Interface Layer (DataService)
│   │                               └── Core Logic:
│   │                                   • Implements the generated Protobuf gRPC base class compiled specs.
│   │                                   • `GetDataPoint`: Queries the closest past or future record for a single point.
│   │                                   • `GetDataRange`: Handles time-bounded pagination windows and returns an 
│   │                                     ISO-8601 string continuation token based on the last row timestamp.
│   │                                     It is suitable for simple paging, but exact same-timestamp collisions are not 
│   │                                     fully disambiguated yet.
│   │                                   • `StreamData`: Executes a polling live tailing loop. It yields records 
│   │                                     immediately when present, sleeps for a fixed interval when idle, and monitors 
│   │                                     `context.is_active()` to free resources instantly if a client drops.
│   │
│   ├── db.py                       # TimeScaleDB Reader Client Layer
│   │                               └── Core Logic:
│   │                                   • Manages a thread-safe `ThreadedConnectionPool` to isolate concurrent read operations.
│   │                                   • Wraps all queries in safe `with` context managers to prevent socket leaks.
│   │                                   • Implements index-optimized, time-series SQL lookups (`ORDER BY time DESC LIMIT 1`).
│   │
│   ├── config.yaml                 # gRPC Server Deployment Profiles
│   │                               └── Details: Network port bindings (default gRPC port), pool capacities, 
│   │                                            and localized database reader access credentials.
│   │
│   └── requirements.txt            # DWS Service Dependencies (grpcio, grpcio-tools, protobuf, psycopg2-binary).
│
├── tests/                          # VALIDATION & AGNOSTIC MOCKING LAYER
│   │                               # Full test suite running on isolated paths without live database sockets.
│   │
│   ├── test_connector.py           # Ingestion Validation Suite
│   │                               └── Test Coverage:
│   │                                   • Data conversion mappings (numbers, strings, and complex serialized JSON arrays).
│   │                                   • Edge cases for telemetry faults (handling sensor values like `NaN` and `+/- Infinity`).
│   │                                   • Queue pressure capabilities (validating drop-safe boundaries during buffer overflow).
│   │                                   • Micro-batch worker clock timing, chunking, and group transaction flushes.
│   │
│   └── test_dws.py                 # Retrieval Validation Suite
│   │                               └── Test Coverage:
│   │                                   • Relational query database tuple conversion to Protobuf formats.
│   │                                   • Safe server responses during "cold boot" states (empty tables).
│   │                                   • Pagination boundaries during high-frequency timestamp collisions.
│   │                                   • Live stream polling back-offs and graceful connection drop resource cleanups.
│   │                                   • Threaded connection pool leak protection during transaction failures.
│   │
└── timescale_build.sh              # INFRASTRUCTURE DEPLOYMENT AUTOMATION
                                    └── Details: Script to spin up localized Docker multi-stage structures, build out the 
                                                 underlying PostgreSQL container, enable the Timescale time-series extension, 
                                                 and configure the operational data hypertables automatically.

```

## Prerequisites

* Docker & Docker Compose
* `psql` client (for manual verification)
* Python 3.12+ virtual environment

---

## Getting Started

### 1. Start TimeScaleDB Engine

Create a .env file and add a variable `DB_PORT`:`your_desired_port_no`.
You can also directly add the port number in the timescale_build.sh file.
If port number is not provided it defaults to 5432.

```bash
bash timescale_build.sh

```

This script initializes a localized TimeScaleDB container instance. If your automation setup does not automatically build out the relational constraints, execute the target schema layout manually inside your SQL shell:

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS timeseries_data (
    time        TIMESTAMPTZ NOT NULL,
    topic       TEXT NOT NULL,
    component   TEXT NOT NULL,
    metric      TEXT NOT NULL,
    value_num   DOUBLE PRECISION,
    value_text  TEXT,
    value_json  JSONB
);

SELECT create_hypertable('timeseries_data', 'time', if_not_exists => TRUE);

```

Although it may not happen, but if prompted for a password during db setup, you can find the password in timescale_build.sh

### 2. Start MQTT Broker Infrastructure

Spin up a localized isolated Eclipse-Mosquitto engine instance to route your industrial network messaging:

```bash
docker run -d --name mqtt -p 1883:1883 eclipse-mosquitto:2

```

---

## Service Operations

### TimescaleDB Node All Dependencies Download

```bash
pip install -r requirements.txt

```

### Ingestion Connector

The connector listens to the default root branch (`mfi-v1.0-historian/#`), translates binary Sparkplug payloads into flat database structures, and queues them into dynamic macro-transactions.

1. Configure your target database environment variables and broker targets inside `connector/config.yaml`.
2. Installing Connector dependencies:
```bash
pip install -r connector/requirements.txt

```

3. Run the worker process:
```bash
cd src/mfi_ddb/databases/timescaledb
PYTHONPATH=. python -m connector.main

```

### DWS gRPC Engine Service

Exposes database reads via safe API endpoint calls.

1. Configure port bindings and connection parameters inside `dws/config.yaml`.
2. Installing DWS dependencies:
```bash
pip install -r dws/requirements.txt

```

3. Execute the service module from inside the `timescaledb/` directory so relative imports resolve cleanly:
```bash
cd src/mfi_ddb/databases/timescaledb
PYTHONPATH=. python -m dws.server

```


---

## Verification & Testing

### Executing the Test Suite

The testing engine is designed to run isolated from the larger monolithic repository structure. To execute the 17 production-ready unit and integration tests (validating memory boundaries, timestamp collisions, connection pool leaks, and network dropouts), navigate into this `timescaledb/` directory and execute `pytest`:


```bash
cd src/mfi_ddb/databases/timescaledb

```

```bash
PYTHONPATH=. pytest tests/ --import-mode=importlib

```

### Useful Pytest Flags

* **View standard output (`print()` statements):**
```bash
PYTHONPATH=. pytest tests/ --import-mode=importlib -s

```


* **Run a specific test module:**
```bash
PYTHONPATH=. pytest tests/test_connector.py

```


### Manual Database Query Verification

To check raw data tables directly inside the live table engines, execute a shell script snapshot query:

```bash
PGPASSWORD=timescale psql -h localhost -U tsdb -d ddb_ts \
    -c "SELECT time, topic, metric, value_num, value_text FROM timeseries_data ORDER BY time DESC LIMIT 10;"

```
