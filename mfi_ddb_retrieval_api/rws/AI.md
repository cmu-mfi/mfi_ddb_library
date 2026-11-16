# Experiment RAG and MCP

This optional module implements the agreed diagram: a sync coordinator inside RWS
discovers completed experiments from MDS, reads their data through the existing DWS
clients, prepares evidence, embeds it, and maintains **one search index in the existing
MDS PostgreSQL database**. The assistant uses a real MCP client to call a read-only
MCP server. Document ingestion, maintenance models, and machine-control tools are
outside this implementation.

## What runs

- RWS background coordinator: settles terminal trials, claims durable jobs, retries
  failures, and periodically reconciles successful trials for late-arriving data.
- Indexing: trial metadata, declared topics, exact numeric summaries, and text/JSON
  observations are normalized and chunked. Original measurements stay in the nodes.
- `ddb_ai.chunks`: text, pgvector embeddings, full-text search, trial references,
  source versions, and indexing timestamps. There is no new database container.
- Retrieval: current trial/project permissions are applied **before** vector and
  keyword ranking; reciprocal rank fusion combines the results. No ANN index is
  required for the initial corpus; exact cosine search preserves filtered recall.
- MCP tools: `discover_trials`, `read_trial`, `search_experiments`. Tool arguments
  never select the caller's identity, and no MCP tool writes to DDB.
- Assistant: bounded tool loop with an external compatible model server, plus a
  small browser chat/evidence interface.

## Docker setup

Use the normal pipeline with the optional AI overlay. The overlay builds pgvector
into the same PostgreSQL 15 Alpine base already used by MDS, and adds AI dependencies
to the RWS image. It does not start a model server or download model weights.

1. Configure a compatible embedding server. For example, a local Ollama instance
   can serve `/v1/embeddings` and `/v1/chat/completions`. Pull `nomic-embed-text` for
   the default 768-dimensional embeddings, and a tool-capable chat model of your
   choice. Leave `DDB_AI_CHAT_MODEL` empty to use search and MCP without generated chat.
2. Create a token file **outside version control**. The user/domain must match an
   existing MDS user. For example, run this after replacing the identity values:

   ```sh
   python3 - <<'PY'
   import json, secrets
   from pathlib import Path
   path = Path('/tmp/ddb-ai-tokens.json')
   path.write_text(json.dumps([{
       'token': secrets.token_urlsafe(32),
       'user_id': 'YOUR_EXISTING_DDB_USER',
       'domain': 'YOUR_EXISTING_DDB_DOMAIN',
       'admin': False,
   }], indent=2))
   path.chmod(0o600)
   PY
   ```

   Each token binds one identity. Add mappings for additional users. `admin: true`
   permits requesting a background sync poll; it does not grant additional trial
   access. Trial ownership and project `admin`/`maintainer` access follow RWS rules.
   Mapping a token to the existing `superadmin/superadmin` identity grants its
   existing unrestricted access, so assign that identity deliberately.
3. Copy `docker/ai/ai.env.example` to a private environment file. Set the absolute
   token-file path, model URL, embedding model/dimension, and optional chat model.
   From the repository root, start the services you use, for example:

   ```sh
   docker compose --env-file /absolute/path/to/ai.env \
     -f docker/docker-compose.yaml -f docker/ai/compose.ai.yaml \
     --profile retrieval --profile kv --profile ts --profile mqtt-broker up -d --build
   ```

4. Open `http://localhost:8000/mfi-ddb/ai/ui` and enter the token from your private
   token file. Check the connection, search indexed experiments, or ask a question.
   The token is kept in page memory and sent as an Authorization bearer header.

The existing MDS tables must have been initialized by the metadata connector.
If RWS starts before those tables exist, restart RWS after connector initialization.
AI startup creates only the `vector` extension and the `ddb_ai` schema/tables.
Keep your normal database backup procedure when changing an existing deployment.

## Local development

From `mfi_ddb_retrieval_api/rws`:

```sh
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-ai.txt
export DDB_AI_ENABLED=true
export DDB_AI_TOKEN_FILE=/absolute/path/to/ddb-ai-tokens.json
export DDB_AI_BASE_URL=http://localhost:11434/v1
export DDB_AI_EMBEDDING_MODEL=nomic-embed-text
export DDB_AI_EMBEDDING_DIMENSIONS=768
# Optional:
export DDB_AI_CHAT_MODEL=your-installed-tool-capable-model
uvicorn main:app --host 127.0.0.1 --port 8000
```

Set `DDB_AI_PG_CONFIG` to a PostgreSQL INI file (an absolute path is accepted), or
edit `app/config/pg_database.ini`. The PostgreSQL server needs pgvector installed.
Set `app/config/dws.endpoints.yaml` to reachable gRPC host/port pairs; metadata DB
connections are not gRPC DWS endpoints. AI data reads use per-topic requests,
deadlines, fresh cursors per source, and explicit page/point limits.

## Interfaces

All AI API calls and MCP HTTP requests require `Authorization: Bearer <token>`.
The UI HTML is public but exposes no private data without a valid token.

| Interface | Purpose |
| --- | --- |
| `GET /mfi-ddb/ai/status` | Models and caller-visible synchronization counts |
| `POST /mfi-ddb/ai/search` | `query`, optional `project_id`, `trial_uuid`, `limit` |
| `POST /mfi-ddb/ai/chat` | `question`, optional bounded user/assistant history |
| `POST /mfi-ddb/ai/sync` | Administrator request to wake the background poller |
| `/mcp/` | Streamable HTTP MCP endpoint; configure bearer headers in your MCP host |

`examples/ai_mcp_client.py` demonstrates an authenticated SDK client. Set
`DDB_AI_TOKEN`, optionally `DDB_MCP_URL`, then pass a search question as arguments.
The application orchestrator uses the same SDK/tools in-process, with the same
server-bound identity. MCP 2.3.0 uses the modern protocol; legacy-only hosts need a
compatible host/client version. For remote hosts, set `DDB_AI_MCP_HOSTS` to the
actual host allowlist (for example `ddb.example.org,ddb.example.org:*`). Configure
`DDB_RWS_CORS_ORIGINS` for your browser client and use your normal HTTPS gateway.
Existing `/type0`–`/type3` authentication semantics are unchanged; new tokens apply
to the AI surface, not retroactively to the legacy API.

## Synchronization semantics and limits

- Eligible trials have `clean_exit IS TRUE`, an end at/after birth, and metadata
  that has settled for `DDB_AI_SETTLE_SECONDS` (default 30). A populated end timestamp
  alone is insufficient. New trial insertion now leaves end time null until death.
- Each declared non-blob topic must return data. Missing topics, unavailable nodes,
  repeated cursors, or exceeded limits fail the job without committing partial
  evidence. Blob/file contents are intentionally excluded.
- Settling plus periodic reconciliation is eventual consistency, **not** a guarantee
  that every connector has flushed at the terminal metadata event. Trials with
  legitimately empty declared topics remain pending retries under this policy.
- A source checksum includes metadata, fetched values, embedding model/dimension,
  and chunking settings. Unchanged data avoids repeated embedding calls.
- One database transaction replaces a trial's evidence and acknowledges its version.
  Leases prevent duplicate workers; expired leases can be reclaimed after a crash.
- Defaults: poll 30 seconds, reconcile 900 seconds, retry 60 seconds, 10 trials per
  batch, 300 seconds per trial, 100,000 retrieved points, 1,000 pages, 100 topics,
  256 chunks. Exceeding limits is an explicit failure rather than silent truncation.
  Prefix the corresponding settings with `DDB_AI_`; see `app/ai/config.py`.
- `read_trial` selects up to 24 hours within an accessible trial and returns exact
  summaries plus at most 20 example samples per signal. It accepts declared topics
  only. Units come from source metadata; the assistant must not invent them.
- Search returns ranked evidence even for weak matches; the generator is instructed
  to assess relevance and abstain when unsupported. Retrieval and generated answers
  still need evaluation on your experiments. Search results alone are not predictions.
- Changing embedding models/dimensions is rejected against an existing index.
  Stop AI workers and explicitly rebuild the derived `ddb_ai` schema before changing
  the embedding configuration. Original trial/source tables are not part of that rebuild.

## Verification

Install the AI requirements and run, from `rws`:

```sh
pytest tests
```

Tests use fake model/DWS responses for deterministic orchestration checks. For real
PostgreSQL/pgvector tests, set `DDB_AI_TEST_DSN` to a disposable server connection:

```sh
DDB_AI_TEST_DSN='host=localhost port=5432 dbname=postgres user=postgres password=...' pytest tests
```

The integration fixture creates uniquely named test databases and removes only
those databases afterward. It requires `CREATEDB` and pgvector on that test server.
It verifies permission filtering/revocation, completion detection, exclusive leases,
transaction rollback, and end-to-end sync into actual vector/full-text search.

References: [MCP Python SDK](https://py.sdk.modelcontextprotocol.io/),
[pgvector](https://github.com/pgvector/pgvector),
[Ollama compatible API](https://docs.ollama.com/api/openai-compatibility).
