"""Authenticate the AI surface without accepting model-supplied identities."""

import hashlib
import secrets
from contextvars import ContextVar

from app.ai.config import Principal
from starlette.responses import JSONResponse

current_principal: ContextVar[Principal | None] = ContextVar(
    "ddb_ai_principal", default=None
)


def principal() -> Principal:
    value = current_principal.get()
    if value is None:
        raise PermissionError("An authenticated AI identity is required")
    return value


class AIAuthMiddleware:
    """Pure ASGI middleware preserves identity across async MCP/tool calls."""

    def __init__(self, app, tokens):
        self.app = app
        self.tokens = [
            (hashlib.sha256(k.encode()).digest(), v) for k, v in tokens.items()
        ]

    async def __call__(self, scope, receive, send):
        path = scope.get("path", "")
        protected = path == "/mcp" or path.startswith(("/mcp/", "/mfi-ddb/ai/"))
        if (
            scope["type"] != "http"
            or not protected
            or path == "/mfi-ddb/ai/ui"
            or scope["method"] == "OPTIONS"
        ):
            return await self.app(scope, receive, send)
        headers = dict(scope.get("headers", []))
        authorization = headers.get(b"authorization", b"").decode("latin-1")
        scheme, _, token = authorization.partition(" ")
        digest = hashlib.sha256(token.encode()).digest()
        identity = None
        if scheme.lower() == "bearer":
            for expected, candidate in self.tokens:
                if secrets.compare_digest(expected, digest):
                    identity = candidate
        if identity is None:
            response = JSONResponse(
                {"detail": "Valid AI bearer token required"},
                401,
                headers={"WWW-Authenticate": "Bearer"},
            )
            return await response(scope, receive, send)
        context = current_principal.set(identity)
        try:
            await self.app(scope, receive, send)
        finally:
            current_principal.reset(context)
