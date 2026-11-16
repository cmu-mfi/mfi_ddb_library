"""Deterministic experiment evidence; calculations happen before generation."""

import hashlib
import json
import math
import statistics
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import NAMESPACE_URL, uuid5


def canonical(value):
    return json.dumps(
        value, sort_keys=True, ensure_ascii=False, default=str, allow_nan=False
    )


def utc(value):
    if isinstance(value, str):
        value = datetime.fromisoformat(value)
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def numeric_summary(points):
    numbers = [
        p["value"]
        for p in points
        if isinstance(p["value"], (int, float))
        and not isinstance(p["value"], bool)
        and math.isfinite(p["value"])
    ]
    result = {"sample_count": len(points), "numeric_count": len(numbers)}
    if points:
        ordered = sorted(points, key=lambda p: utc(p["timestamp"]))
        result.update(
            time_start=ordered[0]["timestamp"],
            time_end=ordered[-1]["timestamp"],
            first=ordered[0]["value"],
            last=ordered[-1]["value"],
        )
    if numbers:
        result.update(
            minimum=min(numbers),
            maximum=max(numbers),
            mean=statistics.fmean(numbers),
            standard_deviation=statistics.pstdev(numbers),
        )
    return result


@dataclass(frozen=True)
class Chunk:
    id: str
    content: str
    provenance: dict


def prepare_chunks(trial, data, settings):
    trial_id = str(trial["uuid"])
    common = {
        "trial_uuid": trial_id,
        "trial_name": trial.get("trial_name"),
        "project_id": str(trial["project_id"]) if trial.get("project_id") else None,
        "time_start": str(trial["birth_timestamp"]),
        "time_end": str(trial["death_timestamp"]),
    }
    metadata = {
        **common,
        "metadata": trial.get("metadata") or {},
        "data_topics": trial.get("data_topics") or [],
    }
    documents = [
        (f"Trial metadata\n{canonical(metadata)}", {**common, "kind": "metadata"})
    ]
    for topic, points in sorted(data.items()):
        summary = numeric_summary(points)
        text = f"Trial: {trial.get('trial_name')} ({trial_id})\nTopic: {topic}\nMeasured summary: {canonical(summary)}"
        # Numeric measurements are represented by exact backend summaries. Text/JSON
        # observations are retained separately and split into bounded evidence chunks.
        observations = [
            p
            for p in points
            if not isinstance(p["value"], (int, float)) or isinstance(p["value"], bool)
        ]
        if observations:
            text += "\nObservations:\n" + "\n".join(canonical(p) for p in observations)
        documents.append(
            (text, {**common, "kind": "signal", "topic": topic, "summary": summary})
        )
    source = {
        "trial": metadata,
        "data": data,
        "embedding": settings.embedding_signature,
        "pipeline": "trial-rag-v1",
        "chunk_chars": settings.chunk_chars,
        "overlap": settings.chunk_overlap,
    }
    version = hashlib.sha256(canonical(source).encode()).hexdigest()
    chunks = []
    step = settings.chunk_chars - settings.chunk_overlap
    for text, provenance in documents:
        for offset in range(0, len(text), step):
            content = text[offset : offset + settings.chunk_chars]
            identity = f"ddb:{trial_id}:{version}:{len(chunks)}"
            chunks.append(
                Chunk(
                    str(uuid5(NAMESPACE_URL, identity)),
                    content,
                    {**provenance, "source_version": version, "offset": offset},
                )
            )
            if len(chunks) > settings.max_chunks:
                raise ValueError(
                    "Experiment exceeds AI_MAX_CHUNKS; raise the configured limit or narrow its topics"
                )
            if offset + settings.chunk_chars >= len(text):
                break
    return version, chunks
