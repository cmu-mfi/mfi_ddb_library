# Contributing to MFI DDB Library

First off, thank you for considering a contribution to the MFI Digital Data Backbone (DDB) Library!

This document describes how to structure, document, and submit your contributions. For an overview of the project and its architecture, see the [README](README.md).

---

## Table of Contents

- [Repository layout](#repository-layout)
- [One PR, one module](#one-pr-one-module)
- [Exception: chore PRs](#exception-chore-prs)
- [Commit messages](#commit-messages)
- [Pull request templates](#pull-request-templates)
- [Web UI and user application contributions](#web-ui-and-user-application-contributions)
- [README maintenance](#readme-maintenance)
- [Disclosure of AI involvement](#disclosure-of-ai-involvement)

---

## Repository layout

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

## One PR, one module

Keep every pull request focused on **a single module**. A PR should contain contributions to one module only — for example, a change to `mfi_ddb_package`, or to `docker`, or to `setup-wizard`.

If your change spans multiple modules, split it into separate PRs, one per module. Open the dependent PRs with references to each other so reviewers can follow the ordering (e.g., "depends on #123").

## Exception: chore PRs

Chores such as documentation updates, linting, formatting, CI configuration, and other housekeeping may touch multiple modules in a single PR.

However, a chore PR **must not include any functional changes**. If you need to fix code while doing chores, put the functional change in its own module-scoped PR (see above).

## Commit messages

Write good commit messages and follow the [Seven Rules of a Great Git Commit Message](https://cbea.ms/git-commit/#seven-rules).

## Pull request templates

Always use one of the pull request templates. When you open a PR, GitHub will prompt you to choose a template — pick the one that matches your contribution:

- 🐞 Bug Fix
- 🚀 Feature
- 🌟 Enhancement
- 🔌 Data Adapter
- Others

Fill out every applicable section, including the checklist items.

## Web UI and user application contributions

If your contribution adds or changes a **Web UI or a user application** — for example, the Data Adapter App frontend (`mfi_ddb_data_adapter/data_adapter_app/frontend`) or the Setup Wizard UI (`setup-wizard/ui`) — please include **screenshots (images/GIF)** of the contribution in the PR description.

- Embed the images directly in the PR description (GitHub supports drag-and-drop image upload).
- Show the relevant states of the UI: before/after for changes, or the new screens/features for additions.
- Keep GIFs short and focused on the feature being demonstrated.

## README maintenance

Each module and submodule must have a `README.md` file documenting it. If your contribution changes anything relevant to a module's setup, usage, behavior, or structure, update the affected `README.md` files as part of your PR.

## Disclosure of AI involvement

Disclose significant AI involvement. If AI tools played a major role in producing your contribution, say so in the issue, pull request description or a top-level PR comment. We are not interested in incidental use, such as asking a chatbot for advice or accepting an autocomplete suggestion — but rather in cases where a tool generated substantial portions of the work.
