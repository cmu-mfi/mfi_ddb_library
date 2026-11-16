"""Real pgvector integration. DDB_AI_TEST_DSN must point to a disposable test server.

Each test creates and drops its own uniquely named database. No existing tables
or project databases are modified. The server must have pgvector available.
"""

import json
import os
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, Mock
from uuid import uuid4

import httpx
import psycopg2
import pytest
from app.ai.config import Principal, Settings
from app.ai.content import prepare_chunks
from app.ai.provider import ModelProvider
from app.ai.repository import Repository
from app.ai.sync import SyncCoordinator
from main import create_app
from psycopg2 import sql


@pytest.fixture
def repository(monkeypatch):
    dsn = os.getenv("DDB_AI_TEST_DSN")
    if not dsn:
        pytest.skip("Set DDB_AI_TEST_DSN to run real PostgreSQL/pgvector tests")
    admin = psycopg2.connect(dsn)
    admin.autocommit = True
    name = "ddb_ai_test_" + uuid4().hex
    with admin.cursor() as cur:
        cur.execute(
            sql.SQL("CREATE DATABASE {} TEMPLATE template0 ENCODING 'UTF8'").format(
                sql.Identifier(name)
            )
        )
    params = {**admin.get_dsn_parameters(), "dbname": name}
    settings = replace(
        Settings(),
        embedding_model="test-embedding",
        embedding_dimensions=3,
        settle_seconds=1,
    )
    monkeypatch.setattr("app.ai.repository.load_config", lambda _: params)
    conn = psycopg2.connect(**params)
    with conn, conn.cursor() as cur:
        cur.execute("""CREATE TABLE trial (
            uuid uuid PRIMARY KEY, trial_name text, user_id text, user_domain text, project_id uuid,
            birth_timestamp timestamptz, death_timestamp timestamptz, clean_exit boolean,
            metadata jsonb, data_topics text[], created_at timestamptz DEFAULT now(),
            updated_at timestamptz DEFAULT now());
            CREATE TABLE user_project_role_linking (
            project_id uuid,user_id text,domain text,role text);""")
    repo = Repository(settings)
    repo.initialize()
    try:
        yield repo, conn
    finally:
        repo.close()
        conn.close()
        with admin.cursor() as cur:
            cur.execute(
                sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(name))
            )
        admin.close()


def insert_trial(conn, owner="alice", clean_exit=True, end=True):
    identifier, project = str(uuid4()), str(uuid4())
    with conn, conn.cursor() as cur:
        cur.execute(
            """INSERT INTO trial(uuid,trial_name,user_id,user_domain,project_id,birth_timestamp,
            death_timestamp,clean_exit,metadata,data_topics,updated_at)
            VALUES(%s,'temperature experiment',%s,'lab',%s,now()-interval '2 hours',
            CASE WHEN %s THEN now()-interval '1 hour' ELSE NULL END,%s,
            '{"device":"spindle"}',ARRAY['kv/temperature'],now()-interval '1 hour') RETURNING *""",
            (identifier, owner, project, end, clean_exit),
        )
    return identifier, project


def index(repo, identifier, data=None):
    t = repo.trial(identifier, Principal("superadmin", "superadmin"))
    version, chunks = prepare_chunks(t, data or {}, repo.settings)
    token = repo.claim(identifier)
    assert token
    repo.complete(identifier, token, version, chunks, [[1, 0, 0] for _ in chunks])
    return version, chunks


def test_search_filters_current_permissions_before_ranking(repository):
    repo, conn = repository
    alice, project = insert_trial(conn)
    bob, _ = insert_trial(conn, "bob")
    index(repo, alice)
    index(repo, bob)
    results = repo.search(Principal("alice", "lab"), "temperature", [1, 0, 0], 10)
    assert results and {str(hit["trial_uuid"]) for hit in results} == {alice}
    assert not repo.search(Principal("alice", "other-domain"), "temperature", [1, 0, 0])
    assert not repo.search(Principal("outsider", "lab"), "temperature", [1, 0, 0])
    with conn, conn.cursor() as cur:
        cur.execute(
            "INSERT INTO user_project_role_linking VALUES(%s,'reviewer','lab','maintainer')",
            (project,),
        )
    assert repo.search(Principal("reviewer", "lab"), "temperature", [1, 0, 0])
    with conn, conn.cursor() as cur:
        cur.execute("DELETE FROM user_project_role_linking")
    assert not repo.search(Principal("reviewer", "lab"), "temperature", [1, 0, 0])


def test_completion_detection_and_exclusive_crash_recoverable_lease(repository):
    repo, conn = repository
    completed, _ = insert_trial(conn)
    insert_trial(conn, clean_exit=False)
    insert_trial(conn, end=False)
    assert [str(t["uuid"]) for t in repo.candidates()] == [completed]
    token = repo.claim(completed)
    assert token and repo.claim(completed) is None
    with conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE ddb_ai.sync_state SET lease_until=now()-interval '1 second'"
        )
    replacement = repo.claim(completed)
    assert replacement and replacement != token
    with pytest.raises(RuntimeError, match="superseded"):
        repo.complete(completed, token, "stale")


def test_index_replacement_and_sync_ack_are_atomic(repository):
    repo, conn = repository
    identifier, _ = insert_trial(conn)
    old_version, _ = index(repo, identifier)
    with conn, conn.cursor() as cur:
        cur.execute("UPDATE ddb_ai.sync_state SET next_attempt_at=now()")
    token = repo.claim(identifier)
    t = repo.trial(identifier, Principal("alice", "lab"))
    version, chunks = prepare_chunks(
        t,
        {"kv/new": [{"timestamp": t["birth_timestamp"].isoformat(), "value": 7}]},
        repo.settings,
    )
    with pytest.raises(psycopg2.Error):
        repo.complete(identifier, token, version, chunks, [[1, 0] for _ in chunks])
    assert repo.is_current(identifier, old_version)
    assert not repo.is_current(identifier, version)
    repo.complete(identifier, token, version, chunks, [[0, 1, 0] for _ in chunks])
    assert not repo.is_current(identifier, old_version)
    assert repo.is_current(identifier, version)
    assert repo.status(Principal("alice", "lab"))[0]["status"] == "synced"


@pytest.mark.anyio
async def test_complete_sync_to_real_vector_search(repository):
    repo, conn = repository
    identifier, _ = insert_trial(conn)
    dws = Mock()
    dws.get_data_bounded.return_value = {
        "kv/temperature": [
            {
                "timestamp": (
                    datetime.now(UTC) - timedelta(hours=1, minutes=1)
                ).isoformat(),
                "value": 12,
            }
        ]
    }
    provider = Mock()
    provider.embed = AsyncMock()

    async def embed(texts):
        return [[1, 0, 0] for _ in texts]

    provider.embed.side_effect = embed
    counts = await SyncCoordinator(repo, dws, provider, repo.settings).run_once()
    assert counts["synced"] == 1
    hits = repo.search(Principal("alice", "lab"), "temperature", [1, 0, 0])
    assert hits and all(str(hit["trial_uuid"]) == identifier for hit in hits)
    assert any('"mean": 12.0' in hit["content"] for hit in hits)


@pytest.mark.anyio
async def test_full_runtime_sync_search_and_chat_with_real_postgres(
    repository, monkeypatch
):
    repo, conn = repository
    identifier, _ = insert_trial(conn)
    identity_token = "integration-test-token-with-sufficient-length"
    settings = replace(
        repo.settings,
        enabled=True,
        sync_enabled=False,
        chat_model="test-chat",
        tokens={identity_token: Principal("alice", "lab")},
    )

    def model_response(request):
        payload = json.loads(request.content)
        if request.url.path.endswith("embeddings"):
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"index": index, "embedding": [1, 0, 0]}
                        for index, _ in enumerate(payload["input"])
                    ]
                },
            )
        previous = payload["messages"][-1]
        if previous["role"] == "tool":
            evidence = json.loads(previous["content"])["evidence"]
            message = {
                "role": "assistant",
                "content": "The indexed experiment evidence is available ["
                + evidence[0]["citation_id"]
                + "].",
            }
        else:
            message = {
                "role": "assistant",
                "content": None,
                "tool_calls": [
                    {
                        "id": "integration-call",
                        "type": "function",
                        "function": {
                            "name": "search_experiments",
                            "arguments": '{"query":"temperature"}',
                        },
                    }
                ],
            }
        return httpx.Response(200, json={"choices": [{"message": message}]})

    monkeypatch.setattr(
        "app.ai.runtime.ModelProvider",
        lambda settings: ModelProvider(
            settings,
            httpx.AsyncClient(
                base_url="http://model/", transport=httpx.MockTransport(model_response)
            ),
        ),
    )
    monkeypatch.setattr(
        "main.DwsAgent.get_data_bounded",
        lambda *args, **kwargs: {
            "kv/temperature": [{"timestamp": "2026-01-01T00:00:01Z", "value": 12}]
        },
    )
    app = create_app(settings, metadata_factory=lambda **kwargs: Mock())
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://localhost:8000",
            headers={"Authorization": f"Bearer {identity_token}"},
        ) as client,
    ):
        counts = await app.state.ai.coordinator.run_once()
        assert counts["synced"] == 1
        search = await client.post("/mfi-ddb/ai/search", json={"query": "temperature"})
        assert search.status_code == 200
        assert search.json()["evidence"][0]["trial_uuid"] == identifier
        chat = await client.post(
            "/mfi-ddb/ai/chat", json={"question": "What was collected?"}
        )
        assert chat.status_code == 200
        assert chat.json()["citations"][0]["trial_uuid"] == identifier
        assert chat.json()["tool_results"][0]["tool"] == "search_experiments"
