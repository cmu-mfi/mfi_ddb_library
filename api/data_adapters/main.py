"""
Main FastAPI application for DDB Unified API service.
Fixed version with proper route registration.
"""

import os
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Import the router and lifespan from your config module
from api.data_adapters.routers.config import router, lifespan


app = FastAPI(
    title="DDB Unified API",
    version="1.0.0",
    description="Core API for data adapter management",
    lifespan=lifespan
)
# Enable CORS for development
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000", 
        "http://localhost:3001",
        "http://127.0.0.1:3000",
        "*"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register configuration routes with /config prefix
app.include_router(router, prefix="/config", tags=["Configuration"])

# Add a root health check
@app.get("/")
async def root():
    return {"status": "healthy", "service": "DDB Unified API"}

# Serve frontend in production
build_dir = os.path.join(
    os.path.dirname(__file__),
    "..", "ui_interfaces", "data_adapters", "build"
)
if os.path.isdir(build_dir):
    app.mount("/static", StaticFiles(directory=build_dir, html=True), name="ui")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)