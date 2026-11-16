"""HTTP adapter for locally hosted or compatible embedding/chat providers."""

import math

import httpx


class ModelProvider:
    def __init__(self, settings, client=None):
        self.settings = settings
        self.client = client or httpx.AsyncClient(
            base_url=settings.base_url.rstrip("/") + "/",
            timeout=httpx.Timeout(90, connect=10),
            headers={"Authorization": f"Bearer {settings.api_key}"},
        )

    async def close(self):
        await self.client.aclose()

    async def embed(self, texts):
        vectors = []
        for offset in range(0, len(texts), 32):
            batch = texts[offset : offset + 32]
            response = await self.client.post(
                "embeddings",
                json={
                    "model": self.settings.embedding_model,
                    "input": batch,
                },
            )
            response.raise_for_status()
            items = sorted(response.json()["data"], key=lambda x: x["index"])
            if [x["index"] for x in items] != list(range(len(batch))):
                raise ValueError(
                    "Embedding provider returned incomplete or duplicate results"
                )
            for item in items:
                vector = item["embedding"]
                if (
                    len(vector) != self.settings.embedding_dimensions
                    or not all(
                        isinstance(x, (int, float))
                        and not isinstance(x, bool)
                        and math.isfinite(x)
                        for x in vector
                    )
                    or not any(vector)
                ):
                    raise ValueError(
                        "Embedding provider returned invalid vectors or an unexpected dimension"
                    )
                vectors.append(vector)
        return vectors

    async def complete(self, messages, tools=None):
        if not self.settings.chat_model:
            raise ValueError("Set DDB_AI_CHAT_MODEL to enable generated answers")
        payload = {
            "model": self.settings.chat_model,
            "messages": messages,
            "temperature": 0,
            "max_tokens": 1200,
        }
        if tools:
            payload["tools"] = tools
        response = await self.client.post("chat/completions", json=payload)
        response.raise_for_status()
        return response.json()["choices"][0]["message"]
