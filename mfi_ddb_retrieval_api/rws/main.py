"""
Retrieval API - Main Application Entry Point
"""

import sys
import time
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware

# ==============================================================================
# 1. INITIALIZE TELEMETRY FIRST (Must run before loading database modules)
# ==============================================================================
from prometheus_client import start_http_server
from opentelemetry.exporter.prometheus import PrometheusMetricReader
from opentelemetry.metrics import set_meter_provider, get_meter
from opentelemetry.sdk.metrics import MeterProvider

# Spin up Prometheus exporter on port 9464
reader = PrometheusMetricReader()
provider = MeterProvider(metric_readers=[reader])
set_meter_provider(provider)
start_http_server(port=9464, addr="0.0.0.0")
print("Prometheus metrics server listening on port 9464")

# Initialize global API meter
meter = get_meter("mfi.rws.app")

# Global HTTP metrics
http_requests_counter = meter.create_counter(
    "mfi_rws_http_requests_total",
    description="Total HTTP requests processed by Retrieval API"
)

http_request_duration = meter.create_histogram(
    "mfi_rws_http_request_duration_seconds",
    description="Latency of HTTP requests in seconds"
)
# ==============================================================================

# Now we can safely import our local modules that will use telemetry
from app.api.v0.router import router
from app.services.pg_mds import MdsReader

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: initialize metadata_reader
    config_file = 'pg_database.test.ini' if 'pytest' in sys.modules else 'pg_database.ini'
    app.state.metadata_reader = MdsReader(config_file=config_file)
    yield
    # Shutdown: cleanup
    if hasattr(app.state, 'metadata_reader'):
        del app.state.metadata_reader
        
app = FastAPI(
    title="Retrieval API",
    version="0.1.0",
    description="Core API for data retrieval from MFI DDB Data store",
    lifespan=lifespan
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
    allow_origins=["*"],  # Adjust for production security if needed
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/mfi-ddb", tags=["MFI DDB"])

@app.get("/")
async def root():
    return {"status": "healthy", "service": "MFI DDB: RETRIEVAL API"}

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)