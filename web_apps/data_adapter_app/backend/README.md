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