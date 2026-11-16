import json
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import httpx2
import pytest
from app.ai.assistant import Assistant
from app.ai.auth import current_principal, principal
from app.ai.config import Principal, Settings
from app.ai.mcp_server import create_mcp
from fastapi.testclient import TestClient
from main import create_app
from mcp import Client
from mcp.client.streamable_http import streamable_http_client

ALICE = "alice-token-which-is-long-enough"
BOB = "bob-token-which-is-long-enough"


class Service:
    async def discover(self, *args):
        return {"trials": [], "user": principal().user_id}

    async def search(self, *args):
        return {
            "evidence": [
                {
                    "citation_id": "evidence-1",
                    "content": "Mean temperature was 10 C.",
                    "user": principal().user_id,
                }
            ]
        }


class FakeRuntime:
    def __init__(self, settings, dws):
        self.settings = settings
        self.service = Service()
        self.mcp = create_mcp(self.service)
        self.repository = SimpleNamespace(
            status=lambda identity: [{"user": identity.user_id}]
        )
        self.coordinator = SimpleNamespace(wake_event=Mock())
        self.start = AsyncMock()
        self.close = AsyncMock()


@pytest.fixture
def client():
    settings = replace(
        Settings(),
        enabled=True,
        tokens={ALICE: Principal("alice", "lab"), BOB: Principal("bob", "lab", True)},
        chat_model="configured",
    )
    app = create_app(
        settings, metadata_factory=lambda **kwargs: Mock(), runtime_factory=FakeRuntime
    )
    with TestClient(app, base_url="http://localhost:8000") as client:
        yield client


def test_ai_requires_token_and_cannot_accept_user_identity(client):
    assert client.get("/mfi-ddb/ai/ui").status_code == 200
    assert client.get("/mfi-ddb/ai/status").status_code == 401
    assert client.get("/mcp/").status_code == 401
    headers = {"Authorization": f"Bearer {ALICE}"}
    assert client.get("/mfi-ddb/ai/status", headers=headers).json()["trials"] == [
        {"user": "alice"}
    ]
    assert (
        client.post(
            "/mfi-ddb/ai/search",
            headers=headers,
            json={"query": "temperature", "user_id": "superadmin"},
        ).status_code
        == 422
    )
    result = client.post(
        "/mfi-ddb/ai/search", headers=headers, json={"query": "temperature"}
    )
    assert result.json()["evidence"][0]["user"] == "alice"
    assert client.post("/mfi-ddb/ai/sync", headers=headers).status_code == 403
    assert (
        client.post(
            "/mfi-ddb/ai/sync", headers={"Authorization": f"Bearer {BOB}"}
        ).status_code
        == 200
    )


def test_real_http_mcp_transport_binds_request_identity(client):
    def invoke(token):
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json, text/event-stream",
            "Mcp-Protocol-Version": "2026-07-28",
            "Mcp-Method": "tools/call",
            "Mcp-Name": "discover_trials",
        }
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {
                "name": "discover_trials",
                "arguments": {},
                "_meta": {
                    "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                    "io.modelcontextprotocol/clientCapabilities": {},
                    "io.modelcontextprotocol/clientInfo": {
                        "name": "test",
                        "version": "1",
                    },
                },
            },
        }
        response = client.post("/mcp/", headers=headers, json=body)
        assert response.status_code == 200, response.text
        result = response.json()["result"]
        assert not result.get("isError", False), result
        return result["structuredContent"]["user"]

    assert invoke(ALICE) == "alice"
    assert invoke(BOB) == "bob"


@pytest.mark.anyio
async def test_chat_executes_actual_mcp_tools_and_returns_citations():
    provider = SimpleNamespace(
        complete=AsyncMock(
            side_effect=[
                {
                    "role": "assistant",
                    "content": None,
                    "tool_calls": [
                        {
                            "id": "call_1",
                            "type": "function",
                            "function": {
                                "name": "search_experiments",
                                "arguments": json.dumps({"query": "temperature"}),
                            },
                        }
                    ],
                },
                {
                    "role": "assistant",
                    "content": "The mean temperature was 10 C [evidence-1].",
                },
            ]
        )
    )
    assistant = Assistant(create_mcp(Service()), provider, Settings())
    context = current_principal.set(Principal("alice", "lab"))
    try:
        response = await assistant.chat("What was the temperature?")
    finally:
        current_principal.reset(context)
    assert response["citations"][0]["citation_id"] == "evidence-1"
    assert response["citations"][0]["user"] == "alice"
    assert provider.complete.call_count == 2


@pytest.mark.anyio
async def test_model_that_skips_tools_is_given_retrieved_evidence():
    provider = SimpleNamespace(
        complete=AsyncMock(return_value={"role": "assistant", "content": "Answer"})
    )
    assistant = Assistant(create_mcp(Service()), provider, Settings())
    context = current_principal.set(Principal("alice", "lab"))
    try:
        response = await assistant.chat("temperature")
    finally:
        current_principal.reset(context)
    assert response["tool_results"][0]["tool"] == "search_experiments"
    assert response["citations"]


def test_ai_is_optional_and_existing_endpoints_are_registered():
    app = create_app(Settings(), metadata_factory=lambda **kwargs: Mock())
    with TestClient(app) as client:
        assert client.get("/").json()["ai_enabled"] is False
        assert client.get("/mfi-ddb/type0").status_code == 200
        assert client.get("/mfi-ddb/ai/ui").status_code == 404


@pytest.mark.anyio
async def test_authenticated_sdk_client_over_http_transport():
    settings = replace(
        Settings(), enabled=True, tokens={ALICE: Principal("alice", "lab")}
    )
    app = create_app(
        settings, metadata_factory=lambda **kwargs: Mock(), runtime_factory=FakeRuntime
    )
    async with (
        app.router.lifespan_context(app),
        httpx2.AsyncClient(
            transport=httpx2.ASGITransport(app=app),
            headers={"Authorization": f"Bearer {ALICE}"},
        ) as http,
        Client(
            streamable_http_client("http://localhost:8000/mcp/", http_client=http)
        ) as client,
    ):
        result = await client.call_tool("discover_trials", {})
        assert result.structured_content["user"] == "alice"
