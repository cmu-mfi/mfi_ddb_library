

### Generated deployment configuration

The backend writes submitted YAML documents under
`~/.mfi_ddb_runtime/runtime_configs/` and generates a Compose file mounting them.
Local KV/Timescale database initialization follows the submitted connection settings.
Connector and DWS settings must agree when they use the same local database;
external database settings remain independent. Existing data is never reset to
apply changed initialization credentials: migrate the database first.

KV DWS uses its configured port. Timescale, Blob, and AVEVA listen on internal
port 50051 and publish host ports 50052, 50053, and 50054. RWS routes between
containers must use internal ports. The wizard publishes RWS at
`http://localhost:8002` to avoid the setup backend on port 8000.

Blob connector and DWS paths mount the same host directory. The DWS index path
must be `<blob_dir>/index.jsonl`, matching the connector's fixed index filename.

Backend regression checks (install backend requirements and `httpx` first):
`python -m unittest discover -s setup-wizard/backend/tests -v`.
The Compose matrix test requires the Docker CLI but does not start containers.
