import asyncio
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch
from uuid import uuid4

import httpx
import pytest
from app.ai.auth import current_principal, principal
from app.ai.config import Principal, Settings
from app.ai.content import numeric_summary, prepare_chunks
from app.ai.mcp_server import create_mcp
from app.ai.provider import ModelProvider
from app.ai.service import AIService
from app.ai.sync import SyncCoordinator
from app.services.dws_agent import DwsReadError, __DwsAgent
from app.utils.dws.gen.models_pb2 import Datapoint
from app.utils.dws.gen.service_pb2 import GetDataRangeResponse
from mcp import Client


def trial():
    return {
        "uuid": str(uuid4()),
        "trial_name": "Spindle temperature experiment",
        "project_id": None,
        "birth_timestamp": datetime(2026, 1, 1, tzinfo=UTC),
        "death_timestamp": datetime(2026, 1, 1, tzinfo=UTC) + timedelta(hours=1),
        "clean_exit": True,
        "data_topics": ["kv/temperature"],
        "metadata": {"units": "C"},
    }


def points():
    return [
        {"timestamp": "2026-01-01T00:00:01Z", "value": 0},
        {"timestamp": "2026-01-01T00:00:02Z", "value": 20},
    ]


def test_configuration_is_explicit(monkeypatch):
    monkeypatch.delenv("DDB_AI_ENABLED", raising=False)
    assert not Settings.from_env().enabled
    monkeypatch.setenv("DDB_AI_ENABLED", "true")
    monkeypatch.delenv("DDB_AI_TOKEN_FILE", raising=False)
    monkeypatch.setenv("DDB_AI_TOKENS", "[]")
    with pytest.raises(ValueError, match="requires"):
        Settings.from_env()

    monkeypatch.setenv(
        "DDB_AI_TOKENS",
        '[{"token":"a-secret-token-with-32-characters","user_id":"alice","domain":"lab"}]',
    )
    assert (
        Settings.from_env().tokens["a-secret-token-with-32-characters"].user_id
        == "alice"
    )
    monkeypatch.setenv("DDB_AI_CHUNK_OVERLAP", "9999")
    with pytest.raises(ValueError, match="overlap"):
        Settings.from_env()


def test_new_trial_has_no_end_timestamp(monkeypatch):
    import importlib
    from pathlib import Path

    monkeypatch.syspath_prepend(
        str(Path(__file__).resolve().parents[2] / "metadata_store_pg")
    )
    module = importlib.import_module("pg_mds")
    connector = module.MdsConnector.__new__(module.MdsConnector)
    connector._MdsConnector__conn_pool = None
    connector._upsert = Mock(return_value={})
    connector.insert_trial("running", ("alice", "lab"), "2026-01-01T00:00:00Z")
    assert connector._upsert.call_args.args[1]["death_timestamp"] is None


def test_chunks_are_reproducible_and_measurements_are_calculated():
    t, settings = trial(), Settings()
    version, chunks = prepare_chunks(t, {"kv/temperature": points()}, settings)
    repeated = prepare_chunks(t, {"kv/temperature": points()}, settings)
    assert repeated == (version, chunks)
    summary = numeric_summary(points())
    assert (
        summary["mean"] == 10 and summary["minimum"] == 0 and summary["maximum"] == 20
    )
    assert '"mean": 10.0' in chunks[1].content
    changed, _ = prepare_chunks(t, {"kv/temperature": points()[:1]}, settings)
    assert changed != version
    assert (
        prepare_chunks(t, {}, replace(settings, embedding_model="another-model"))[0]
        != version
    )
    assert all(c.provenance["source_version"] == version for c in chunks)


def test_chunk_limit_fails_instead_of_silently_dropping_content():
    with pytest.raises(ValueError, match="MAX_CHUNKS"):
        prepare_chunks(
            trial(),
            {},
            replace(Settings(), chunk_chars=30, chunk_overlap=5, max_chunks=1),
        )


@pytest.mark.anyio
async def test_embedding_provider_reorders_results_and_checks_dimensions():
    def handler(request):
        return httpx.Response(
            200,
            json={
                "data": [
                    {"index": 1, "embedding": [0, 1, 0]},
                    {"index": 0, "embedding": [1, 0, 0]},
                ]
            },
        )

    client = httpx.AsyncClient(
        base_url="http://model/", transport=httpx.MockTransport(handler)
    )
    provider = ModelProvider(replace(Settings(), embedding_dimensions=3), client)
    assert await provider.embed(["one", "two"]) == [[1, 0, 0], [0, 1, 0]]
    with pytest.raises(ValueError, match="incomplete"):
        await provider.embed(["one"])
    await provider.close()


@pytest.mark.anyio
async def test_failed_embedding_leaves_sync_failed_and_does_not_commit():
    t = trial()
    repo = Mock()
    repo.candidates.return_value = [t]
    repo.claim.return_value = "lease"
    repo.is_current.return_value = False
    dws = Mock()
    dws.get_data_bounded.return_value = {"kv/temperature": points()}
    provider = SimpleNamespace(embed=AsyncMock(side_effect=RuntimeError("offline")))
    coordinator = SyncCoordinator(repo, dws, provider, Settings())
    result = await coordinator.run_once()
    assert result["failed"] == 1
    repo.complete.assert_not_called()
    repo.fail.assert_called_once()


@pytest.mark.anyio
async def test_missing_declared_data_is_retried_before_embedding():
    repo = Mock()
    repo.candidates.return_value = [trial()]
    repo.claim.return_value = "lease"
    dws = Mock()
    dws.get_data_bounded.return_value = {}
    provider = SimpleNamespace(embed=AsyncMock())
    assert (await SyncCoordinator(repo, dws, provider, Settings()).run_once())[
        "failed"
    ] == 1
    provider.embed.assert_not_called()
    repo.complete.assert_not_called()


@pytest.mark.anyio
async def test_reconciliation_skips_unchanged_embeddings_but_records_checked_data():
    repo = Mock()
    repo.candidates.return_value = [trial()]
    repo.claim.return_value = "lease"
    repo.is_current.return_value = True
    dws = Mock()
    dws.get_data_bounded.return_value = {"kv/temperature": points()}
    provider = SimpleNamespace(embed=AsyncMock())
    assert (await SyncCoordinator(repo, dws, provider, Settings()).run_once())[
        "synced"
    ] == 1
    provider.embed.assert_not_called()
    assert len(repo.complete.call_args.args) == 3


@pytest.mark.anyio
async def test_actual_mcp_tools_keep_concurrent_user_identities_separate():
    class Service:
        async def discover(self, *args):
            await asyncio.sleep(0)
            return {"user": principal().user_id}

    server = create_mcp(Service())

    async def call(name):
        context = current_principal.set(Principal(name, "lab"))
        try:
            async with Client(server) as client:
                tools = (await client.list_tools()).tools
                assert {t.name for t in tools} == {
                    "discover_trials",
                    "read_trial",
                    "search_experiments",
                }
                assert all(
                    t.annotations.read_only_hint and not t.annotations.destructive_hint
                    for t in tools
                )
                result = await client.call_tool(
                    "discover_trials", {"user_id": "superadmin"}
                )
                assert not result.is_error
                assert result.structured_content == {"user": name}
                invalid = await client.call_tool(
                    "search_experiments", {"query": "test", "limit": 10000}
                )
                assert invalid.is_error
        finally:
            current_principal.reset(context)

    await asyncio.gather(call("alice"), call("bob"))


@pytest.mark.anyio
async def test_read_trial_checks_permissions_before_touching_source():
    repository, dws = Mock(), Mock()
    repository.trial.return_value = None
    service = AIService(repository, dws, None, Settings())
    token = current_principal.set(Principal("alice", "lab"))
    try:
        with pytest.raises(LookupError):
            await service.read_trial(str(uuid4()))
        dws.get_data_bounded.assert_not_called()
    finally:
        current_principal.reset(token)


def agent():
    obj = __DwsAgent.__new__(__DwsAgent)
    obj.config = {
        "kv": [
            {"url": "http://node:50051", "name": "one"},
            {"url": "node:50052", "name": "two"},
        ]
    }
    return obj


def response(topic="kv/temperature", nanos=0, token=""):
    point = Datapoint(topic=topic, float_value=12)
    point.timestamp.FromJsonString("2026-01-01T00:00:01Z")
    point.timestamp.nanos = nanos
    return GetDataRangeResponse(datapoints=[point], next_page_token=token)


def test_dws_resets_cursor_per_source_and_preserves_subsecond_samples():
    obj = agent()
    with patch.object(
        obj,
        "_call_dws_server",
        side_effect=[response(token="next"), response(nanos=100), response()],
    ) as call:
        data = obj.get_data_bounded(
            ["kv/temperature"], "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
        )
    assert [c.args[1].page_token for c in call.call_args_list] == ["", "next", ""]
    assert len(data["kv/temperature"]) == 2


@pytest.mark.parametrize("scenario", ["loop", "limit", "offscope", "unknown"])
def test_dws_rejects_incomplete_or_out_of_scope_retrieval(scenario):
    obj = agent()
    source = response(
        topic="kv/secret" if scenario == "offscope" else "kv/temperature",
        token="loop" if scenario == "loop" else "",
    )
    with (
        patch.object(obj, "_call_dws_server", return_value=source),
        pytest.raises(DwsReadError),
    ):
        obj.get_data_bounded(
            ["unknown/topic" if scenario == "unknown" else "kv/temperature"],
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z",
            max_points=1 if scenario == "limit" else 100,
        )


def test_dws_reads_each_topic_individually():
    obj = agent()
    obj.config["kv"] = obj.config["kv"][:1]
    with patch.object(
        obj,
        "_call_dws_server",
        side_effect=[response("kv/current"), response("kv/temperature")],
    ) as call:
        data = obj.get_data_bounded(
            ["kv/current", "kv/temperature"],
            "2026-01-01T00:00:00Z",
            "2026-01-01T01:00:00Z",
        )
    assert [c.args[1].topic for c in call.call_args_list] == [
        "kv/current",
        "kv/temperature",
    ]
    assert set(data) == {"kv/current", "kv/temperature"}


def test_grpc_endpoint_normalization_and_deadline():
    obj = agent()
    with (
        patch("app.services.dws_agent.grpc.insecure_channel") as channel,
        patch("app.services.dws_agent.DataServiceStub") as stub,
    ):
        request = Mock()
        obj._call_dws_server("http://node:50051", request)
    channel.assert_called_once_with("node:50051")
    stub.return_value.GetDataRange.assert_called_once_with(request, timeout=10)


def test_dws_stops_pagination_when_total_time_budget_expires():
    obj = agent()
    with (
        patch("app.services.dws_agent.time.monotonic", side_effect=[0, 0, 61]),
        patch.object(
            obj, "_call_dws_server", return_value=response(token="next")
        ) as call,
        pytest.raises(DwsReadError, match="time budget"),
    ):
        obj.get_data_bounded(
            ["kv/temperature"], "2026-01-01T00:00:00Z", "2026-01-01T01:00:00Z"
        )
    assert call.call_count == 1
