# Data Adapter Application Interface

REST API for Data Adapters in the MFI DDB Library.

## 📁 Project Structure

```
backend
├── app
│   ├── api
│   │   ├── __init__.py
│   │   └── v0
│   │       └── router.py       # Main API router with endpoints
│   ├── main.py                 # FastAPI application entry point
│   ├── services
│   │   └── adapter_factory.py
│   └── utils
│       └── utils.py
├── pytest.ini
├── README.md                   # This documentation
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


### Prerequisites & Installation

**Requirements:**
- Python 3.9 or higher
- pip package manager
- Virtual environment (recommended)

**Installation Steps:**

```bash
cd mfi_ddb_library/web_apps/data_adapter_app/backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Install mfi_ddb library
cd ../../../
pip install .
```

## API Endpoints

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| **GET** | `/adapters` | Retrieves a list of all discovered adapters along with their metadata, schemas, and configuration examples. |
| **GET** | `/health` | Returns the service operational status, current timestamp, and counts of active/streaming connections. |
| **POST** | `/validate/adapter` | Validates an adapter's YAML configuration (via file or text) against its specific JSON schema. |
| **POST** | `/validate/streamer` | Validates the streamer's YAML configuration to ensure compatibility with MQTT Sparkplug B settings. |
| **POST** | `/connect/{conn_id}` | Initializes an adapter instance and starts data streaming for a specific connection ID. |
| **POST** | `/resume/{conn_id}` | Restarts data streaming for an existing connection that has been previously paused. |
| **POST** | `/pause/{conn_id}` | Suspends data streaming for a specific connection while keeping the adapter instance active in memory. |
| **POST** | `/disconnect/{conn_id}` | Stops streaming, closes the adapter connection, and removes the instance from the active registry. |
| **GET** | `/streaming-status/{conn_id}` | Provides real-time connectivity and streaming state details for a specific connection ID. |