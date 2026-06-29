# Blob DWS (Data Warehouse Service)

A gRPC-based data warehouse service for storing and retrieving binary blob files, with support for MQTT-based ingestion and a PI Web API data source.

---

## Project Structure

```
blob/
├── connector/
│   ├── connector.py        # MQTT subscriber — ingests blob messages and saves to disk
│   └── config.yaml         # Connector config (broker address, topic, save directory)
├── dws/
│   ├── gen/           # gRPC server — serves blob data to clients
│   │   ├── models.pb2_grpc.py
│   │   ├── models.pb2.py
│   │   ├── service.pb2_grpc.py
│   │   └── service.pb2.py 
│   ├── proto/           # gRPC server — serves blob data to clients
│   │   ├── models.proto
│   │   └── server.proto
│   ├── server.py           # gRPC server — serves blob data to clients
│   ├── blobapi.py          # Core blob storage API (read/query logic)
│   ├── error_codes.py      # gRPC error code definitions
│   └── config.yaml         # DWS server config (blob_dir, index_path)
├── gen/
│   ├── models_pb2.py       # Generated protobuf models
│   ├── service_pb2.py      # Generated protobuf service
│   └── service_pb2_grpc.py # Generated gRPC service
└── tests/
    ├── test_blobapi_unit.py          # Unit tests for BlobAPI (mocked, no real files)
    ├── test_blobapi_integration.py   # Integration tests for BlobAPI (real index.jsonl)
    ├── test_connector_unit.py        # Unit tests for connector (mocked, no MQTT)
    └── test_connector_integration.py # Integration tests for connector (live MQTT)
```

---

## Components

### Connector (`connector/connector.py`)
Subscribes to an MQTT broker and listens for blob messages. On receiving a message it:
- Validates the message format
- Saves the blob file to disk
- Saves a metadata `.json` file
- Appends an entry to `index.jsonl`

### DWS Server (`dws/server.py`)
A gRPC server exposing three endpoints:
- `GetDataPoint` — returns the blob closest to a requested timestamp
- `GetDataRange` — returns a paginated list of blobs within a time range
- `StreamData` — not yet implemented

### BlobAPI (`dws/blobapi.py`)
Core read logic used by the DWS server. Reads from `index.jsonl` and loads blob files from disk. Supports:
- Exact and wildcard topic matching (`sensors/temp` or `sensors/#`)
- Closest-past or closest-future lookup
- Paginated range queries with per-topic token tracking


## Configuration

### Connector (`connector/config.yaml`)
```yaml
mqtt:
  broker_address: "127.0.0.1"
  broker_port: 1883
  username: "username"
  password: "password"
  tls_enabled: false
  debug: false

config:
  save_directory: "/path/to/blob/storage"
  topic: "mfi-v1.0-blob/site/#"
```

### DWS Server (`dws/config.yaml`)
```yaml
config:
  blob_dir: "/path/to/blob/storage"
  index_path: "/path/to/blob/storage/index.jsonl"
```

---

## Running

### Start the Connector
```bash
python connector/connector.py connector/config.yaml
```

### Start the DWS Server
```bash
cd dws
python server.py
```

---

## Storage Format

All blobs are stored in a flat directory with three file types per blob:

| File | Description |
|---|---|
| `<file_id>.<ext>` | The raw blob file |
| `<file_id>.json` | Metadata (topic, timestamp, trial_id, file_type) |
| `index.jsonl` | One JSON record per blob for fast querying |

### index.jsonl record format
```json
{
  "file_id": "abc123def456",
  "trial_id": "trial_1",
  "timestamp": "1704067200.0",
  "topic": "mfi-v1.0-blob/CMU/filesystem/data",
  "file_type": "jpg"
}
```

---

## Testing

### Run all unit tests
```bash
pytest -v -s tests/test_blobapi_unit.py tests/test_connector_unit.py
```

### Run BlobAPI integration tests
Requires a real `index.jsonl` and blob files in `tests/test_output/`:
```bash
pytest -v -s tests/test_blobapi_integration.py
```

### Run connector integration tests
Requires a real MQTT broker and `connector/config.yaml`:
```bash
pytest -v -s tests/test_connector_integration.py
```

### Run all tests
```bash
pytest -v -s tests/
```

Unit tests use synthetic in-memory data and run without any external dependencies. Integration tests skip automatically if the required files or broker are not available.

---

## gRPC API

### GetDataPoint
Returns the single blob closest to the requested timestamp for a given topic.

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic name, supports wildcard (`/#`) |
| `timestamp` | Timestamp | Target timestamp |
| `do_closest_past` | bool | If true, returns closest past; if false, closest future |

### GetDataRange
Returns a paginated list of blobs within a time range.

| Field | Type | Description |
|---|---|---|
| `topic` | string | Topic name, supports wildcard (`/#`) |
| `start_time` | Timestamp | Range start (exclusive) |
| `end_time` | Timestamp | Range end (inclusive) |
| `page_size` | int32 | Results per page (default: 1000) |
| `page_token` | string | Pagination token from previous response |

