"""Example: DDB_AI_TOKEN=... python examples/ai_mcp_client.py 'temperature trials'."""

import asyncio
import os
import sys

import httpx2
from mcp import Client
from mcp.client.streamable_http import streamable_http_client


async def main():
    url = os.getenv("DDB_MCP_URL", "http://localhost:8000/mcp/")
    token = os.environ["DDB_AI_TOKEN"]
    async with (
        httpx2.AsyncClient(headers={"Authorization": f"Bearer {token}"}) as http,
        Client(streamable_http_client(url, http_client=http)) as client,
    ):
        result = await client.call_tool(
            "search_experiments",
            {"query": " ".join(sys.argv[1:]) or "available experiments"},
        )
        for item in result.content:
            if item.type == "text":
                print(item.text)


if __name__ == "__main__":
    asyncio.run(main())
