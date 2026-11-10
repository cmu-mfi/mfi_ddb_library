# AGENTS.md

Instructions for AI coding agents working in this repository.

This is a monorepo consisting of independent projects/modules in separate root
directories (`mfi_ddb_package`, `mfi_ddb_data_adapter`,
`mfi_ddb_database_nodes`, `mfi_ddb_retrieval_api`, `setup-wizard`, `docker`).

| Module | Description |
|--------|-------------|
| [`mfi_ddb_package`](./mfi_ddb_package/) | Core Python package: data adapters, streamers, topic families, and the payload schema |
| [`mfi_ddb_data_adapter`](./mfi_ddb_data_adapter/) | Data Adapter App: FastAPI backend and React frontend for managing data adapters on an edge device |
| [`mfi_ddb_database_nodes`](./mfi_ddb_database_nodes/) | Compatible database storage nodes (e.g., kv-psql, blob, timescaledb, aveva-pi, dws) |
| [`mfi_ddb_retrieval_api`](./mfi_ddb_retrieval_api/) | Retrieval services: Metadata Store (MDS) and Retrieval Web Service (RWS) |
| [`setup-wizard`](./setup-wizard/) | Setup wizard (backend + UI) for deploying and configuring the pipeline |
| [`docker`](./docker/) | Docker Compose configurations for running the full MFI DDB pipeline |


## Working on contributions

* **Before writing any code, read [CONTRIBUTING.md](CONTRIBUTING.md).** Every rule in it applies to you. Re-read it before declaring the task complete.
* **Git Branching**
    * Always work in a new branch targeting a Pull Request (PR) (`git switch -c <type>/<description>`)
    * Unless otherwise specified, create all working branches from `dev`.
* **Planning Checklist**
    * Ask the user which type of PR it is during planning phase (e.g., feature, bug fix, data adapter)
    * Include the corresponding checklist from `.github/PULL_REQUEST_TEMPLATE/` in your task plan.
* **Python modules**
    * Use *uv* package manager for python modules (https://docs.astral.sh/uv/)
    * Prefer using *ruff* commands for linting and formating.
