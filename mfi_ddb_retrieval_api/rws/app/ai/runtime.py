"""Lifecycle wiring; no AI imports or connections are needed when disabled."""

import asyncio

from app.ai.assistant import Assistant
from app.ai.mcp_server import create_mcp
from app.ai.provider import ModelProvider
from app.ai.repository import Repository
from app.ai.service import AIService
from app.ai.sync import SyncCoordinator


class Runtime:
    def __init__(self, settings, dws):
        self.settings = settings
        self.repository = Repository(settings)
        self.provider = ModelProvider(settings)
        self.service = AIService(self.repository, dws, self.provider, settings)
        self.coordinator = SyncCoordinator(
            self.repository, dws, self.provider, settings
        )
        self.mcp = create_mcp(self.service)
        self.assistant = Assistant(self.mcp, self.provider, settings)
        self.task = None

    async def start(self):
        await asyncio.to_thread(self.repository.initialize)
        if self.settings.sync_enabled:
            self.task = asyncio.create_task(self.coordinator.run(), name="ddb-rag-sync")

    async def close(self):
        self.coordinator.stop_event.set()
        self.coordinator.wake_event.set()
        if self.task:
            await self.task
        await self.provider.close()
        await asyncio.to_thread(self.repository.close)
