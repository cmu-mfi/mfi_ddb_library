"""Shared authorized operations used by HTTP and MCP alike."""

import asyncio
from datetime import UTC, datetime, timedelta

from app.ai.auth import principal
from app.ai.content import numeric_summary, utc
from fastapi.encoders import jsonable_encoder


class AIService:
    def __init__(self, repository, dws, provider, settings):
        self.repository, self.dws, self.provider, self.settings = (
            repository,
            dws,
            provider,
            settings,
        )

    async def discover(self, query="", project_id=None, limit=20):
        result = await asyncio.to_thread(
            self.repository.discover, principal(), query, project_id, limit
        )
        return jsonable_encoder({"trials": result})

    async def search(self, query, limit=5, project_id=None, trial_uuid=None):
        identity = principal()
        embedding = (await self.provider.embed([query]))[0]
        hits = await asyncio.to_thread(
            self.repository.search,
            identity,
            query,
            embedding,
            limit,
            project_id,
            trial_uuid,
        )
        return jsonable_encoder(
            {
                "query": query,
                "evidence": hits,
                "note": "Indexed experiment snapshots; use read_trial for current measurements.",
            }
        )

    async def read_trial(self, trial_uuid, time_start=None, time_end=None, topics=None):
        trial = await asyncio.to_thread(self.repository.trial, trial_uuid, principal())
        if trial is None:
            raise LookupError("Trial not found or inaccessible")
        birth = utc(trial["birth_timestamp"])
        death = (
            utc(trial["death_timestamp"])
            if trial.get("clean_exit") and trial.get("death_timestamp")
            else datetime.now(UTC)
        )
        start = utc(time_start) if time_start else birth
        end = utc(time_end) if time_end else min(death, start + timedelta(days=1))
        if (
            start < birth
            or end > death
            or end < start
            or end - start > timedelta(days=1)
        ):
            raise ValueError("Select a time window inside the trial, at most 24 hours")
        declared = trial.get("data_topics") or []
        selected = declared if topics is None else topics
        if len(selected) > self.settings.max_topics or any(
            topic not in declared for topic in selected
        ):
            raise ValueError(
                "Select only the trial's declared topics, within DDB_AI_MAX_TOPICS"
            )
        selected = [topic for topic in selected if "blob" not in topic.split("/")[0]]
        data = await asyncio.to_thread(
            self.dws.get_data_bounded,
            selected,
            start.isoformat(),
            end.isoformat(),
            max_points=self.settings.max_points,
            max_pages=self.settings.max_pages,
        )
        signals = {}
        for topic, points in data.items():
            signals[topic] = {
                "summary": numeric_summary(points),
                "samples": points[:20],
                "samples_truncated": len(points) > 20,
            }
        return jsonable_encoder(
            {
                "trial_uuid": str(trial_uuid),
                "trial_name": trial.get("trial_name"),
                "metadata": trial.get("metadata") or {},
                "signals": signals,
                "time_start": start,
                "time_end": end,
                "declared_topics": declared,
                "retrieved_at": datetime.now(UTC),
                "note": "Summaries cover all fetched points; samples are limited to 20 per signal. File content is excluded.",
            }
        )
