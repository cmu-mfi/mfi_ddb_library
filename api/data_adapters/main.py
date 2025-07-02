from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from .routers.config import router as config_router
import os

app = FastAPI(
    title="DDB Unified API",
    version="1.0.0",
    description="Unified API for MTConnect, MQTT, File, and ROS: validate, test, connect, stream, publish.",
    docs_url="/docs",
    redoc_url="/redoc",
    swagger_ui_parameters={"defaultModelsExpandDepth": -1}
)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])
app.include_router(config_router, prefix="/config")

build_dir = os.path.join(os.path.dirname(__file__), '..','ui_interfaces', 'data_adapters', 'build')
if os.path.isdir(build_dir):
    app.mount('/', StaticFiles(directory=build_dir, html=True), name='ui')