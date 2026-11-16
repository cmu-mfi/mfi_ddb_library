"""Metadata-driven background synchronization, with retries and reconciliation."""

import asyncio
import logging

from app.ai.content import prepare_chunks
from app.services.dws_agent import topic_matches

logger = logging.getLogger(__name__)


class SyncCoordinator:
    def __init__(self, repository, dws, provider, settings):
        self.repository, self.dws, self.provider, self.settings = (
            repository,
            dws,
            provider,
            settings,
        )
        self.stop_event = asyncio.Event()
        self.wake_event = asyncio.Event()
        self.lock = asyncio.Lock()

    async def index_trial(self, trial, token):
        topics = [
            topic
            for topic in trial.get("data_topics") or []
            if "blob" not in topic.split("/")[0]
        ]
        if len(topics) > self.settings.max_topics:
            raise ValueError("Experiment exceeds DDB_AI_MAX_TOPICS")
        data = await asyncio.to_thread(
            self.dws.get_data_bounded,
            topics,
            str(trial["birth_timestamp"]),
            str(trial["death_timestamp"]),
            max_points=self.settings.max_points,
            max_pages=self.settings.max_pages,
            timeout_seconds=max(1, self.settings.trial_timeout - 1),
        )
        # A terminal metadata event can reach MDS before the node connectors flush.
        # Require data for each declared numeric/KV topic; periodically reconcile
        # successful trials as additional rows may arrive after the settling delay.
        missing = [
            topic
            for topic in topics
            if not any(topic_matches(topic, actual) for actual in data)
        ]
        if missing:
            raise RuntimeError(
                "Awaiting data for declared topics: " + ", ".join(missing)
            )
        version, chunks = prepare_chunks(trial, data, self.settings)
        current = await asyncio.to_thread(
            self.repository.is_current, trial["uuid"], version
        )
        if current:
            await asyncio.to_thread(
                self.repository.complete, trial["uuid"], token, version
            )
            return
        vectors = await self.provider.embed([chunk.content for chunk in chunks])
        await asyncio.to_thread(
            self.repository.complete, trial["uuid"], token, version, chunks, vectors
        )

    async def run_once(self):
        if self.lock.locked():
            return {"busy": True, "synced": 0, "failed": 0}
        async with self.lock:
            counts = {"busy": False, "synced": 0, "failed": 0}
            trials = await asyncio.to_thread(self.repository.candidates)
            for trial in trials:
                if self.stop_event.is_set():
                    break
                token = await asyncio.to_thread(self.repository.claim, trial["uuid"])
                if token is None:
                    continue
                try:
                    async with asyncio.timeout(self.settings.trial_timeout):
                        await self.index_trial(trial, token)
                    counts["synced"] += 1
                except Exception as error:
                    logger.exception("Experiment RAG sync failed for %s", trial["uuid"])
                    await asyncio.to_thread(
                        self.repository.fail,
                        trial["uuid"],
                        token,
                        f"{type(error).__name__}: {error}",
                    )
                    counts["failed"] += 1
            return counts

    async def run(self):
        while not self.stop_event.is_set():
            try:
                await self.run_once()
            except Exception:
                logger.exception(
                    "RAG synchronization poll failed; retrying on the next interval"
                )
            try:
                await asyncio.wait_for(
                    self.wake_event.wait(), self.settings.poll_seconds
                )
            except TimeoutError:
                pass
            self.wake_event.clear()
