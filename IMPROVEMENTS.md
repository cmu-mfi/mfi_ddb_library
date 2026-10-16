| Improvement | File relative to project | Proposed change |
|---|---|---|
| Blocking adapter startup — implemented | `mfi_ddb_data_adapter/data_adapter_app/backend/app/api/v0/router.py` | Lifecycle calls run concurrently in the framework worker pool, with per-connection locks to serialize requests for the same ID.|
| Timescale INSERT overhead — implemented | `mfi_ddb_database_nodes/timescaledb/connector/db.py` | Use `execute_values` for one multi-row INSERT per page of up to 500 rows, reducing repeated SQL statement processing. |
| Topic/time indexing | `mfi_ddb_database_nodes/timescaledb/connector/db.py` | Check existing indexes and query plans; add `(topic, time)` if justified. |
| Connection per request — implemented | `mfi_ddb_database_nodes/kv-psql/dws/server.py` | Reuse a thread-safe connection pool sized to the existing gRPC workers; close cursors and return connections after requests/streams, and close the pool on shutdown. |
| COUNT on every page — implemented | `mfi_ddb_database_nodes/kv-psql/dws/server.py` | Fetch `page_size + 1` rows in one query, return at most `page_size`, and issue a next-page token only when an extra row exists. |
| Full blob-index scans | `mfi_ddb_database_nodes/blob/dws/blobapi.py`, `mfi_ddb_database_nodes/blob/connector/connector.py` | Add SQLite indexing while continuing to maintain JSONL. Fall back to JSONL when SQLite is unavailable or incomplete. **Separate PR if this exceeds the scope limit.** |
| Sequential PI requests — implemented | `mfi_ddb_database_nodes/aveva-pi/dws/piwebapi.py` | Fetch independent streams in parallel using the default bounded worker pool and separate HTTP sessions; merge in stream order to preserve sorting and page-token behavior. Keep single-stream requests on the existing path. |
| Duplicate parsing/validation — implemented | `mfi_ddb_retrieval_api/metadata_store_pg/connector.py` | Load the validator once per process, parse each message once, apply the package defaults, and validate once before database handling. Package code is unchanged. |
| Blocking retrieval API — implemented | `mfi_ddb_retrieval_api/rws/app/api/v0/router.py`, `mfi_ddb_retrieval_api/rws/app/services/dws_agent.py` | Run Type 1/2/3 handlers in FastAPI’s worker pool; protect shared DWS channel/stub initialization with a lock. API routes and response models are unchanged. |
| Permission-query N+1 | `mfi_ddb_retrieval_api/rws/app/services/pg_mds.py` | Batch role lookups while preserving existing authorization rules. |
| Fixed retrieval fan-out | `mfi_ddb_retrieval_api/rws/app/services/dws_agent.py` | Adapt chunk count for small requests. Defer shared-executor lifecycle or larger concurrency restructuring. |

The Type 1/2/3 retrieval routes now use `def` instead of `async def`, so FastAPI automatically runs them in its worker thread pool. Previously, synchronous database/gRPC calls inside `async def` occupied the event loop while waiting. Those calls now wait in worker threads, leaving the event loop available for other requests; the underlying calls remain synchronous.

## Concurrent Adapter Lifecycle

Connect, resume, pause, and disconnect operations run in the framework worker thread pool. Different connection IDs can execute concurrently without an additional application-level concurrency limit. The framework worker pool still bounds how many blocking calls execute at once; excess calls wait for a worker.

Requests for the same `conn_id` execute sequentially, preventing duplicate startup and overlapping lifecycle changes. Health and status endpoints remain responsive while adapters initialize. Idle connection locks are automatically released from the lock registry.

The connection registry and locks are process-local. Use one Uvicorn worker unless requests are routed consistently to the process owning each connection. MQTT connection and initial-data waits still contribute to startup time.
