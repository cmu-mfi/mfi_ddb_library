"""Actual MCP tools: read-only operations with server-bound user identity."""

from typing import Annotated, Any
from uuid import UUID

from mcp.server import MCPServer
from mcp.types import ToolAnnotations
from pydantic import Field

READ_ONLY = ToolAnnotations(
    read_only_hint=True,
    destructive_hint=False,
    idempotent_hint=True,
    open_world_hint=False,
)


def create_mcp(service):
    server = MCPServer(
        "MFI DDB",
        instructions=(
            "Read-only trial discovery, bounded measurements and indexed experiment evidence. "
            "Identity is bound by the host. Never infer failures or causation from similarity alone. "
            "Cite evidence IDs and distinguish indexed snapshots from current measurements."
        ),
        version="1.0.0",
    )

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def discover_trials(
        query: Annotated[str, Field(max_length=300)] = "",
        project_id: UUID | None = None,
        limit: Annotated[int, Field(ge=1, le=50)] = 20,
    ) -> dict[str, Any]:
        """Find accessible trials and their declared signals using metadata text and project filters."""
        return await service.discover(
            query, str(project_id) if project_id else None, limit
        )

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def read_trial(
        trial_uuid: UUID,
        time_start: str | None = None,
        time_end: str | None = None,
        topics: Annotated[list[str], Field(max_length=100)] | None = None,
    ) -> dict[str, Any]:
        """Read an accessible trial's measurements and exact backend statistics. Max window: 24 hours.

        Use ISO 8601 timestamps. Topics must be selected from discover_trials results.
        Numeric summaries cover the fetched window; returned raw samples are bounded.
        """
        return await service.read_trial(str(trial_uuid), time_start, time_end, topics)

    @server.tool(annotations=READ_ONLY, structured_output=True)
    async def search_experiments(
        query: Annotated[str, Field(min_length=1, max_length=2000)],
        limit: Annotated[int, Field(ge=1, le=10)] = 5,
        project_id: UUID | None = None,
        trial_uuid: UUID | None = None,
    ) -> dict[str, Any]:
        """Search authorized completed-experiment snapshots using vector and keyword retrieval.

        Returns cited passages, measurement summaries, source versions and indexing timestamps.
        These are indexed snapshots; use read_trial for fresh measurements.
        """
        return await service.search(
            query,
            limit,
            str(project_id) if project_id else None,
            str(trial_uuid) if trial_uuid else None,
        )

    return server
