"""
Main FastAPI application for DDB Unified API service.
Handles API configuration and serves as entry point for the application.
"""

import os
import yaml                        # pip install pyyaml
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from fastapi.openapi.docs import get_swagger_ui_html
from .routers.config import router as config_router

app = FastAPI(
    title="DDB Unified API",
    version="1.0.0",
    description="Core API for data adapter management",
)

# Enable CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register configuration routes
app.include_router(config_router, prefix="/config", tags=["Configuration"])

# Serve frontend in production
build_dir = os.path.join(
    os.path.dirname(__file__),
    "..", "ui_interfaces", "data_adapters", "build"
)
if os.path.isdir(build_dir):
    app.mount("/", StaticFiles(directory=build_dir, html=True), name="ui")
