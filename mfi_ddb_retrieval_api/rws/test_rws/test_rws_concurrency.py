"""
A slow data retrieval must not freeze the rest of the API.

The data handlers do blocking I/O, so they are plain `def` and FastAPI runs
them in a thread pool. Were they `async def`, a long Type 2 request would block
the event loop and the health check (and every other request) would hang
until it finished. No database or DWS needed: both are faked.

    cd mfi_ddb_retrieval_api/rws
    pytest -v test_rws/test_rws_concurrency.py
"""

import threading
import time
from contextlib import asynccontextmanager

from fastapi.testclient import TestClient

from app.api.v0 import router as router_module
from app.api.v0.router import get_metadata_reader
from main import app

TRIAL = {
    "uuid": "57be8ff9-238f-4ece-81dc-0c7899008d3a",
    "trial_name": "trial_001",
    "project_id": None,
    "project_name": None,
    "metadata": {},
    "data_topics": ["mfi-v1.0-historian/CMU/pump/rms_x"],
    "birth_timestamp": "2026-11-11T16:00:00+00:00",
    "death_timestamp": "2026-11-11T17:00:00+00:00",
    "created_at": "2026-11-11T16:00:00+00:00",
    "updated_at": "2026-11-11T16:00:00+00:00",
}

SLOW_SECONDS = 3


class FakeReader:
    def get_trial_by_uuid(self, uuid, user_id, user_domain):
        return dict(TRIAL)


def test_health_answers_while_a_slow_retrieval_runs(monkeypatch):
    started = threading.Event()

    def slow_get_data(topics, start, end, bytes=False):
        started.set()
        time.sleep(SLOW_SECONDS)  # blocking, like the real gRPC calls
        return {}

    @asynccontextmanager
    async def no_database(_app):
        yield

    app.dependency_overrides[get_metadata_reader] = lambda: FakeReader()
    monkeypatch.setattr(router_module.DwsAgent, "get_data", slow_get_data)
    # `with` runs every request on one shared event loop, like the real server
    # (without it each request gets its own loop and blocking is never seen).
    # The startup hook is swapped out so no real database pool is opened.
    monkeypatch.setattr(app.router, "lifespan_context", no_database)
    with TestClient(app) as client:
        _check_health_during_slow_retrieval(client, started)
    app.dependency_overrides.clear()


def _check_health_during_slow_retrieval(client, started):
    try:
        slow = threading.Thread(target=lambda: client.post("/mfi-ddb/type2", json={
            "trial_uuid": TRIAL["uuid"], "user_id": "superadmin", "user_domain": "superadmin",
            "time_start": "2026-11-11T16:00:00Z", "time_end": "2026-11-11T16:01:00Z",
        }))
        slow.start()
        assert started.wait(5), "the slow retrieval never started"

        t0 = time.monotonic()
        res = client.get("/")
        elapsed = time.monotonic() - t0

        assert res.status_code == 200
        assert elapsed < 1, f"health check waited {elapsed:.1f}s behind the slow retrieval"
        slow.join()
    finally:
        app.dependency_overrides.clear()
