"""Explicit configuration; enabling AI never selects a paid model implicitly."""

import json
import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Principal:
    user_id: str
    domain: str | None
    admin: bool = False


@dataclass(frozen=True)
class Settings:
    enabled: bool = False
    tokens: dict[str, Principal] = field(default_factory=dict, repr=False)
    base_url: str = "http://localhost:11434/v1"
    api_key: str = field(default="ollama", repr=False)
    embedding_model: str = "nomic-embed-text"
    embedding_dimensions: int = 768
    chat_model: str = ""
    pg_config: str = "pg_database.ini"
    sync_enabled: bool = True
    poll_seconds: int = 30
    settle_seconds: int = 30
    reconcile_seconds: int = 900
    retry_seconds: int = 60
    trial_timeout: int = 300
    max_points: int = 100000
    max_pages: int = 1000
    max_topics: int = 100
    chunk_chars: int = 1600
    chunk_overlap: int = 160
    max_chunks: int = 256
    batch_size: int = 10
    max_tool_rounds: int = 5
    mcp_hosts: tuple[str, ...] = ("localhost:*", "127.0.0.1:*", "[::1]:*")

    @property
    def embedding_signature(self):
        return f"{self.embedding_model}:{self.embedding_dimensions}"

    @classmethod
    def from_env(cls):
        env = os.environ
        enabled = env.get("DDB_AI_ENABLED", "false").lower() == "true"
        if not enabled:
            return cls()
        token_text = env.get("DDB_AI_TOKENS", "[]")
        if env.get("DDB_AI_TOKEN_FILE"):
            token_text = Path(env["DDB_AI_TOKEN_FILE"]).read_text()
        tokens = {}
        for item in json.loads(token_text):
            token = item["token"]
            if len(token) < 24 or token in tokens or not item["user_id"]:
                raise ValueError(
                    "AI bearer tokens must be unique, at least 24 characters, and name a user"
                )
            tokens[token] = Principal(
                item["user_id"], item.get("domain"), item.get("admin", False) is True
            )
        if not tokens:
            raise ValueError(
                "AI requires DDB_AI_TOKEN_FILE or DDB_AI_TOKENS with token-to-user mappings"
            )
        kwargs = {"enabled": True, "tokens": tokens}
        names = {
            "BASE_URL": "base_url",
            "API_KEY": "api_key",
            "EMBEDDING_MODEL": "embedding_model",
            "CHAT_MODEL": "chat_model",
            "PG_CONFIG": "pg_config",
        }
        for suffix, name in names.items():
            if f"DDB_AI_{suffix}" in env:
                kwargs[name] = env[f"DDB_AI_{suffix}"]
        integers = {
            "EMBEDDING_DIMENSIONS": "embedding_dimensions",
            "POLL_SECONDS": "poll_seconds",
            "SETTLE_SECONDS": "settle_seconds",
            "RECONCILE_SECONDS": "reconcile_seconds",
            "RETRY_SECONDS": "retry_seconds",
            "TRIAL_TIMEOUT": "trial_timeout",
            "MAX_POINTS": "max_points",
            "MAX_PAGES": "max_pages",
            "MAX_TOPICS": "max_topics",
            "CHUNK_CHARS": "chunk_chars",
            "CHUNK_OVERLAP": "chunk_overlap",
            "MAX_CHUNKS": "max_chunks",
            "BATCH_SIZE": "batch_size",
            "MAX_TOOL_ROUNDS": "max_tool_rounds",
        }
        for suffix, name in integers.items():
            if f"DDB_AI_{suffix}" in env:
                kwargs[name] = int(env[f"DDB_AI_{suffix}"])
        kwargs["sync_enabled"] = (
            env.get("DDB_AI_SYNC_ENABLED", "true").lower() == "true"
        )
        if env.get("DDB_AI_MCP_HOSTS"):
            kwargs["mcp_hosts"] = tuple(
                x.strip() for x in env["DDB_AI_MCP_HOSTS"].split(",") if x.strip()
            )
        result = cls(**kwargs)
        for name in integers.values():
            if getattr(result, name) <= 0:
                raise ValueError(f"AI {name} must be positive")
        if not 0 < result.chunk_overlap < result.chunk_chars:
            raise ValueError("AI chunk overlap must be smaller than chunk size")
        if result.embedding_dimensions > 2000:
            raise ValueError("AI embeddings must have at most 2000 dimensions")
        if not result.base_url.startswith(("http://", "https://")):
            raise ValueError("AI model base URL must be HTTP(S)")
        return result
