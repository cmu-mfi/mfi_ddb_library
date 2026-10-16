| Improvement | File relative to project | Proposed change |
|---|---|---|
| Blocking adapter startup — implemented | `mfi_ddb_data_adapter/data_adapter_app/backend/app/api/v0/router.py` | Lifecycle calls run concurrently in the framework worker pool, with per-connection locks to serialize requests for the same ID.|
| Timescale INSERT overhead | `mfi_ddb_database_nodes/timescaledb/connector/db.py` | Replace `execute_batch` with multi-row `execute_values`. |
| Topic/time indexing | `mfi_ddb_database_nodes/timescaledb/connector/db.py` | Check existing indexes and query plans; add `(topic, time)` if justified. |
| Connection per request | `mfi_ddb_database_nodes/kv-psql/dws/server.py` | Add connection pooling within the existing service structure. |
| COUNT on every page | `mfi_ddb_database_nodes/kv-psql/dws/server.py` | Fetch `page_size + 1` rows to determine whether another page exists. |
| Full blob-index scans | `mfi_ddb_database_nodes/blob/dws/blobapi.py`, `mfi_ddb_database_nodes/blob/connector/connector.py` | Add SQLite indexing while continuing to maintain JSONL. Fall back to JSONL when SQLite is unavailable or incomplete. **Separate PR if this exceeds the scope limit.** |
| Sequential PI requests | `mfi_ddb_database_nodes/aveva-pi/dws/piwebapi.py` | Use bounded parallel requests or an existing batch endpoint, subject to the scope limit. |
| Duplicate parsing/validation | `mfi_ddb_retrieval_api/metadata_store_pg/connector.py` | Cache the validator and parse/validate once within this connector; leave the package untouched. |
| Blocking retrieval API | `mfi_ddb_retrieval_api/rws/app/api/v0/router.py` | Use synchronous handlers or offload blocking database/gRPC calls. |
| Permission-query N+1 | `mfi_ddb_retrieval_api/rws/app/services/pg_mds.py` | Batch role lookups while preserving existing authorization rules. |
| Fixed retrieval fan-out | `mfi_ddb_retrieval_api/rws/app/services/dws_agent.py` | Adapt chunk count for small requests. Defer shared-executor lifecycle or larger concurrency restructuring. |

## Concurrent Adapter Lifecycle

Connect, resume, pause, and disconnect operations run in the framework worker thread pool. Different connection IDs can execute concurrently without an additional application-level concurrency limit. The framework worker pool still bounds how many blocking calls execute at once; excess calls wait for a worker.

Requests for the same `conn_id` execute sequentially, preventing duplicate startup and overlapping lifecycle changes. Health and status endpoints remain responsive while adapters initialize. Idle connection locks are automatically released from the lock registry.

The connection registry and locks are process-local. Use one Uvicorn worker unless requests are routed consistently to the process owning each connection. MQTT connection and initial-data waits still contribute to startup time.
