# Data Adapter Application Interface

REST API for Data Adapters in the MFI DDB Library.

## 📁 Project Structure

```
backend
├── app
│   ├── api
│   │   ├── __init__.py
│   │   └── v0
│   │       └── router.py            # Main API router with endpoints + startup restore
│   ├── main.py                      # FastAPI application entry point
│   ├── services
│   │   ├── adapter_factory.py       # Wraps a data adapter + Streamer as one connection
│   │   └── connection_store.py      # SQLite persistence for connection configs/state
│   └── utils
│       └── utils.py
├── pytest.ini
├── README.md                        # This documentation
├── requirements.txt
└── tests
    └── test_api.py
```

## 🚀 Getting Started

### Running the Backend Server

```bash
uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload
```
The API server will start and be available at `http://localhost:8000`.

Connection state (configs + whether each connection should be streaming/paused/stopped) is persisted to a SQLite file so it survives restarts. It defaults to `data/connections.db` (relative to wherever the process is started from); override the location with the `MFI_DAA_DB_PATH` environment variable.

### Prerequisites & Installation

**Requirements:**
- Python 3.9 or higher
- pip package manager
- Virtual environment (recommended)

**Installation Steps:**

```bash
cd mfi_ddb_data_adapter/data_adapter_app/backend
python -m venv .venv
source .venv/bin/activate      # on Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

> [!Note]
> `requirements.txt` pins `mfi_ddb` from PyPI. If you're working against local changes to `mfi_ddb_package` in this same repo, install that instead, e.g. `pip install -e ../../../mfi_ddb_package`, run *after* the `requirements.txt` install so it takes precedence.

## API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| **GET** | `/all` | Lists every connection this backend process knows about (including ones that are Stopped, not just currently streaming), with full config and live status. The source of truth for the UI's connection list. |
| **GET** | `/adapters` | Retrieves a list of all discovered adapters along with their metadata, schemas, and configuration examples. |
| **GET** | `/streamer` | Streamer configuration metadata (example, help text, JSON schema) - same shape as `/adapters`, for the Streamer config instead of a data adapter. |
| **GET** | `/health` | Returns the service operational status, current timestamp, and counts of active/streaming connections. |
| **POST** | `/validate/adapter` | Validates an adapter's YAML configuration (via file or text) against its specific JSON schema. Does not connect to anything. |
| **POST** | `/validate/streamer` | Validates the streamer's YAML configuration against its schema. Does not connect to anything. |
| **POST** | `/connect` | Creates a brand new connection; the backend generates its id. |
| **POST** | `/connect/{conn_id}` | Reconnects an existing (e.g. previously Stopped) connection using its already-stored config. Does not create new connections - 404s if `conn_id` isn't already known. |
| **POST** | `/update/{conn_id}` | Applies an updated configuration to an existing connection. A change to only `polling_rate_hz` (polling mode) is applied live with no interruption; any other change connects the new config first and only disconnects the old one once the new one succeeds, so a bad edit can't take down a working connection. |
| **POST** | `/resume/{conn_id}` | Resumes streaming on a connection that's connected but paused. |
| **POST** | `/pause/{conn_id}` | Pauses streaming (polling mode only) while keeping the adapter instance alive. |
| **POST** | `/stop/{conn_id}` | Disconnects a connection but keeps it known to the backend (config included), so it still appears in `/all` and can be reconnected without re-entering config. |
| **POST** | `/delete/{conn_id}` | Disconnects (if needed) and fully forgets a connection - removes it from `/all` and the SQLite store entirely. |

> [!Note]
> All mutating endpoints use `POST`, including update and delete, for consistency with the rest of the router rather than using `PUT`/`DELETE` - see `router.py`'s module docstring for the most up-to-date endpoint list if this table and the code ever drift.

## Pytest Unit Tests

There are unit tests available for the API endpoints in the `tests/test_api.py` file.

> [!Warning]
> These tests currently call `/connect/{conn_id}` and `/disconnect/{conn_id}` directly with hardcoded ids (e.g. `POST /connections/connect/100`) to *create* connections. That's no longer how creation works - `/connect/{conn_id}` is reconnect-only now (404s if the id doesn't already exist) and `/disconnect/{conn_id}` was renamed to `/stop/{conn_id}`. The tests need updating to use `POST /connect` (no id) for creation and `/stop/{conn_id}` for disconnecting before they'll pass again.

To run the tests (once updated):

```bash
pytest -v
```
or
```bash
PYTHONPATH=. python tests/test_api.py
```

### Tests

* TEST 1: Get Available Data Adapters
* TEST 2: Validate Data Adapter Configurations
* TEST 3: Validate Streamer Configuration
* TEST 4: Test Data Adapter Connections
* TEST 5: Test Multiple Data Adapter Connections
