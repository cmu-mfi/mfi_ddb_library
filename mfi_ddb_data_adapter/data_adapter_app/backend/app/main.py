"""
DDB Unified API - Main Application Entry Point
"""

import sys
import os
import time
import logging
from pathlib import Path

# ---------- Configure logging immediately ----------
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(LOG_DIR / "app.log"),
    ],
)

logging.getLogger("uvicorn").setLevel(logging.DEBUG)
logging.getLogger("uvicorn.error").setLevel(logging.DEBUG)
logging.getLogger("uvicorn.access").setLevel(logging.INFO)

# ==============================================================================
# 1. INITIALIZE TELEMETRY (Must run before loading adapter frameworks)
# ==============================================================================
from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter
from opentelemetry.sdk.metrics import MeterProvider

reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")

# Initialize global Data Adapter meter
meter = get_meter("mfi.data_adapter.app")

# Global HTTP metrics
http_requests_counter = meter.create_counter(
    "mfi_data_adapter_http_requests_total",
    description="Total HTTP requests processed by Data Adapter App"
)

http_request_duration = meter.create_histogram(
    "mfi_data_adapter_http_request_duration_seconds",
    description="Latency of HTTP requests in seconds"
)
# ==============================================================================

# Now we can safely import FastAPI and application routers
import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from app.api.v0.router import router

logger = logging.getLogger(__name__)
logger.warning("MAIN STARTED: Telemetry and Logging initialized successfully.")

app = FastAPI(
    title="DAA - DATA ADAPTER APP - CORE API",
    version="0.2.0",
    description="Core API for data adapter application",
)

# HTTP Request Telemetry Middleware
@app.middleware("http")
async def monitor_requests(request: Request, call_next):
    start_time = time.perf_counter()
    method = request.method
    path = request.url.path
    
    try:
        response = await call_next(request)
        duration = time.perf_counter() - start_time
        status_code = str(response.status_code)
        
        http_request_duration.record(duration, {"method": method, "path": path})
        http_requests_counter.add(1, {"method": method, "path": path, "status": status_code})
        return response
    except Exception as e:
        duration = time.perf_counter() - start_time
        http_request_duration.record(duration, {"method": method, "path": path})
        http_requests_counter.add(1, {"method": method, "path": path, "status": "500"})
        raise e

# CORS setup
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/connections", tags=["Connections"])

@app.get("/")
async def root():
    return {"status": "healthy", "service": "MFI DDB: DATA ADAPTER APP"}

# Static UI serving
build_dir = os.path.join(
    os.path.dirname(__file__),
    "..", "ui_interfaces", "data_adapters", "build"
)
if os.path.isdir(build_dir):
    app.mount("/static", StaticFiles(directory=build_dir, html=True), name="ui")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)