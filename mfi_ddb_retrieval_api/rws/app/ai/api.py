"""Authenticated HTTP surface for search, chat and synchronization status."""

import logging
from pathlib import Path
from typing import Literal
from uuid import UUID

import httpx
from app.ai.auth import principal
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/mfi-ddb/ai", tags=["DDB AI"])


class SearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    query: str = Field(min_length=1, max_length=2000)
    limit: int = Field(default=5, ge=1, le=10)
    project_id: UUID | None = None
    trial_uuid: UUID | None = None


class HistoryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")
    role: Literal["user", "assistant"]
    content: str = Field(max_length=4000)


class ChatRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    question: str = Field(min_length=1, max_length=2000)
    history: list[HistoryMessage] = Field(default_factory=list, max_length=10)


@router.get("/ui", include_in_schema=False)
async def ui():
    return FileResponse(Path(__file__).with_name("ui.html"))


@router.get("/status")
async def status(request: Request):
    import asyncio

    runtime = request.app.state.ai
    states = await asyncio.to_thread(runtime.repository.status, principal())
    return {
        "embedding_model": runtime.settings.embedding_model,
        "chat_model": runtime.settings.chat_model or None,
        "sync_enabled": runtime.settings.sync_enabled,
        "trials": states,
    }


@router.post("/search")
async def search(body: SearchRequest, request: Request):
    try:
        return await request.app.state.ai.service.search(
            body.query,
            body.limit,
            str(body.project_id) if body.project_id else None,
            str(body.trial_uuid) if body.trial_uuid else None,
        )
    except (httpx.HTTPError, ValueError, KeyError):
        logger.exception("AI embedding request failed")
        raise HTTPException(
            502, "Embedding provider unavailable or invalid; check server configuration"
        ) from None


@router.post("/chat")
async def chat(body: ChatRequest, request: Request):
    if not request.app.state.ai.settings.chat_model:
        raise HTTPException(
            503, "Chat model is not configured; search and MCP tools remain available"
        )
    try:
        return await request.app.state.ai.assistant.chat(body.question, body.history)
    except (httpx.HTTPError, KeyError):
        logger.exception("AI model request failed")
        raise HTTPException(
            502, "Model provider unavailable or returned an invalid response"
        ) from None


@router.post("/sync")
async def sync(request: Request):
    if not principal().admin:
        raise HTTPException(
            403, "An administrator token is required to request a sync poll"
        )
    coordinator = request.app.state.ai.coordinator
    # Wake the background scheduler; indexing work does not run inside this request.
    if not request.app.state.ai.settings.sync_enabled:
        raise HTTPException(
            409, "Enable DDB_AI_SYNC_ENABLED before requesting a background sync"
        )
    coordinator.wake_event.set()
    return {"status": "scheduled"}
