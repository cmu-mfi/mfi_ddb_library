"""
DDB Unified API - Main Application Entry Point

FastAPI application for managing industrial IoT data adapters with real-time MQTT streaming.
Provides adapter-agnostic endpoints for configuration, connection management, and monitoring
of various industrial protocols (MTConnect, MQTT, ROS, Local Files).

Key Features:
- Automatic adapter discovery and registration
- CORS-enabled for frontend integration  
- Static file serving for production UI deployment
- Comprehensive error handling and logging
- Health monitoring and status endpoints
"""

import os
import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import configuration router and application lifespan manager
from app.api.v0.router import router

# FastAPI application instance with metadata and lifecycle management
app = FastAPI(
    title="DAA - DATA ADAPTER APP - CORE API",
    version="0.2.0",
    description="Core API for data adapter application",
    # lifespan=lifespan  # Manages startup/shutdown tasks for adapters and connections
)

# Enables cross-origin requests from React development servers and production deployments
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",   # React development server
        "http://localhost:3001",   # Alternative React port
        "http://127.0.0.1:3000",   # Localhost alternative
        "*"                        # Allow all origins (development only)
    ],
    allow_credentials=True,        # Support authentication cookies/headers
    allow_methods=["*"],           # Allow all HTTP methods
    allow_headers=["*"],           # Allow all headers
)

# Register all configuration management routes under /connections prefix
# This includes adapter discovery, validation, connection management, and monitoring endpoints
app.include_router(router, prefix="/connections", tags=["Connections"])

@app.get("/")
async def root():
    """
    Root endpoint providing basic API information.
    
    Returns:
        Dictionary with service status and name for health monitoring
    """
    return {"status": "healthy", "service": "MFI DDB: DATA ADAPTER APP"}

# Static file serving for production UI deployment
# Serves the built React application from the ui_interfaces build directory
build_dir = os.path.join(
    os.path.dirname(__file__),
    "..", "ui_interfaces", "data_adapters", "build"
)
if os.path.isdir(build_dir):
    # Mount static files with HTML fallback for client-side routing
    app.mount("/static", StaticFiles(directory=build_dir, html=True), name="ui")

# Development server configuration
# Only runs when script is executed directly (not when imported as module)
if __name__ == "__main__":
    # Start development server with auto-reload for code changes
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)