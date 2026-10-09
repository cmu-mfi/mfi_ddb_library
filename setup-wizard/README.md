# Standalone desktop setup wizard

The wizard pulls published container images and starts them with Docker Compose.
It owns its configuration defaults, service definitions, and initialization SQL in
`backend/templates/`. It does not read or package the repository's separate
`docker/` or database-node directories. The `setup-wizard/` directory can be copied
and built on its own.

Users need Docker with Docker Compose installed and running, plus access to the
configured container image registry. The packaged app includes its Python backend;
users do not need Python, Node.js, or this repository.

## Build the desktop app

Build on the target operating system with Python and Node.js installed.
From `setup-wizard/backend/`:

```sh
python -m pip install -r requirements.txt pyinstaller
```

Then from `setup-wizard/ui/`:

```sh
npm install
npm run electron:build
```

`electron:build` rebuilds the Python backend before packaging the UI, so the
installer always includes current backend changes. It uses `python3` on macOS
and Linux, and `python` on Windows; install the dependencies in that interpreter.

PyInstaller bundles `backend/templates/` with the backend executable.
Electron Builder includes `backend/dist/main/` in the desktop installer.
Templates are read from that bundle at runtime; writable configuration files and
initialization SQL are copied into `~/.mfi_ddb_runtime/` before container startup.

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

After the selected stack is ready, **Continue to Dashboard** opens the Data Adapter
App frontend in your browser when that module was selected. The link uses its
generated Compose port mapping (3001 by default) and the setup backend's hostname
or IP, or a specific host IP configured in the frontend port binding. Desktop
deployments default to `http://localhost:3001/`.

Blob connector and DWS paths mount the same host directory. The DWS index path
must be `<blob_dir>/index.jsonl`, matching the connector's fixed index filename.

Backend regression checks (install backend requirements and `httpx` first):
`python -m unittest discover -s setup-wizard/backend/tests -v`.
The Compose matrix test requires the Docker CLI but does not start containers.
