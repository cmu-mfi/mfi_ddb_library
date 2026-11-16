# Contributing to MFI DDB Library

First off, thank you for considering a contribution to the MFI Digital Data Backbone (DDB) Library!

This document describes how to structure, document, and submit your contributions. For an overview of the project and its architecture, see the [README](README.md).

The repository is organized into independent modules, each in its own root directory:

| Module | Description |
|--------|-------------|
| [`mfi_ddb_package`](./mfi_ddb_package/) | Core Python package: data adapters, streamers, topic families, and the payload schema |
| [`mfi_ddb_data_adapter`](./mfi_ddb_data_adapter/) | Data Adapter App: FastAPI backend and React frontend for managing data adapters on an edge device |
| [`mfi_ddb_database_nodes`](./mfi_ddb_database_nodes/) | Compatible database storage nodes (e.g., kv-psql, blob, timescaledb, aveva-pi, dws) |
| [`mfi_ddb_retrieval_api`](./mfi_ddb_retrieval_api/) | Retrieval services: Metadata Store (MDS) and Retrieval Web Service (RWS) |
| [`setup-wizard`](./setup-wizard/) | Setup wizard (backend + UI) for deploying and configuring the pipeline |
| [`docker`](./docker/) | Docker Compose configurations for running the full MFI DDB pipeline |

Each module (and each of its submodules) has its own `README.md` with module-specific details.

Please follow these guidelines:

- **One PR, one module.** A PR should contain contributions to one module only. If your change spans multiple modules, split it into separate PRs, one per module, referencing each other (e.g., "depends on #123").
- **Chore PRs are the exception.** Documentation updates, linting, formatting, CI configuration, and other housekeeping may touch multiple modules, but such a PR **must not include any functional changes**. Fix code in its own module-scoped PR.
- **Write good commit messages.** Follow the [Seven Rules of a Great Git Commit Message](https://cbea.ms/git-commit/#seven-rules).
- **Use the pull request templates.** Pick the one that matches your contribution — 🐞 Bug Fix, 🚀 Feature, 🌟 Enhancement, 🔌 Data Adapter, 🧹 Chores, or Others — and fill out every applicable section, including the checklist items.
- **Include screenshots for Web UI and user application contributions.** If your contribution adds or changes a Web UI or a user application — for example, the Data Adapter App frontend (`mfi_ddb_data_adapter/data_adapter_app/frontend`) or the Setup Wizard UI (`setup-wizard/ui`) — include screenshots (images/GIF) of the contribution in the PR description. Show the relevant states (before/after or new features) and keep GIFs short and focused.
- **Keep READMEs up to date.** Each module and submodule must have a `README.md` documenting it. If your contribution changes anything relevant to a module's setup, usage, behavior, or structure, update the affected `README.md` files as part of your PR.
- **Disclose significant AI involvement.** If AI tools played a major role in producing your contribution, say so in the issue, pull request description or a top-level PR comment. We are not interested in incidental use, such as asking a chatbot for advice or accepting an autocomplete suggestion — but rather in cases where a tool generated substantial portions of the work.
