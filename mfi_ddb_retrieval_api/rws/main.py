"""Retrieval Web Service, with opt-in experiment RAG and read-only MCP."""

import os
import sys
from contextlib import asynccontextmanager

import uvicorn
from app.ai.config import Settings
from app.api.v0.router import router
from app.services.dws_agent import DwsAgent
from app.services.pg_mds import MdsReader
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware


def create_app(ai_settings=None, *, metadata_factory=MdsReader, runtime_factory=None):
    settings = ai_settings if ai_settings is not None else Settings.from_env()
    runtime = None
    if settings.enabled:
        from app.ai.runtime import Runtime

        runtime = (runtime_factory or Runtime)(settings, DwsAgent)

    @asynccontextmanager
    async def lifespan(app):
        default_config = (
            "pg_database.test.ini" if "pytest" in sys.modules else "pg_database.ini"
        )
        config_file = (
            settings.pg_config
            if settings.enabled
            else os.getenv("DDB_RWS_PG_CONFIG", default_config)
        )
        app.state.metadata_reader = metadata_factory(config_file=config_file)
        try:
            if runtime:
                await runtime.start()
                async with runtime.mcp.session_manager.run():
                    yield
            else:
                yield
        finally:
            if runtime:
                await runtime.close()
            app.state.metadata_reader.close()

    app = FastAPI(
        title="Retrieval API",
        version="0.2.0",
        lifespan=lifespan,
        description="MFI DDB retrieval with optional experiment RAG and read-only MCP",
    )
    origins = [
        x.strip()
        for x in os.getenv(
            "DDB_RWS_CORS_ORIGINS",
            "http://localhost:3000,http://localhost:3001,http://127.0.0.1:3000",
        ).split(",")
        if x.strip()
    ]
    app.add_middleware(
        CORSMiddleware,
        allow_origins=origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS"],
        allow_headers=["*"],
    )
    app.include_router(router, prefix="/mfi-ddb", tags=["MFI DDB"])

    if runtime:
        from app.ai.api import router as ai_router
        from app.ai.auth import AIAuthMiddleware
        from mcp.server.transport_security import TransportSecuritySettings

        app.state.ai = runtime
        app.include_router(ai_router)
        mcp_app = runtime.mcp.streamable_http_app(
            streamable_http_path="/",
            stateless_http=True,
            json_response=True,
            transport_security=TransportSecuritySettings(
                allowed_hosts=list(settings.mcp_hosts), allowed_origins=origins
            ),
        )
        app.mount("/mcp", mcp_app)
        app.add_middleware(AIAuthMiddleware, tokens=settings.tokens)

    @app.get("/")
    async def root():
        return {
            "status": "healthy",
            "service": "MFI DDB: RETRIEVAL API",
            "ai_enabled": settings.enabled,
        }

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
