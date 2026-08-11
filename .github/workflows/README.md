# CI Workflows

This repository uses GitHub Actions for continuous integration and automation. Below are the available workflows organized by priority/stage.

## Workflows


| Stage | Job ID             | Job name           | Workflow Name                  | Workflow File                 | Events                                                                                               | Description                                                      |
|-------|--------------------|--------------------|--------------------------------|-------------------------------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| 0     | installation-check | installation-check | MFI-DDB Package CI Checks      | ci-checks.yml                 | push to `dev`, pull_request (mfi_ddb_package/src/\*\*)                                                | Verifies installation setup across Ubuntu and Windows            |
| 0     | streaming-check    | streaming-check    | MFI-DDB Package CI Checks      | ci-checks.yml                 | push to `dev`, pull_request (mfi_ddb_package/src/\*\*)                                                | Tests data adapters (LocalFiles, MQTT, Kv) with EMQX broker     |
| 0     | ros2-check         | ros2-check         | MFI-DDB Package CI Checks      | ci-checks.yml                 | push to `dev`, pull_request (mfi_ddb_package/src/\*\*)                                                | Runs ROS2 tests                                                  |
| 0     | pre-commit-fix     | apply-auto-fixes   | MFI-DDB Package CI Checks      | ci-checks.yml                 | pull_request → mfi_ddb_package, workflow_dispatch                                                     | Auto-fix lint/format via pre-commit (PR branches only)           |
| 1     | check-lockfile     | check-lockfile     | Update uv.lock                 | update-uv-lock.yml            | pull_request (pyproject.toml), workflow_dispatch                                                    | Checks and updates uv.lock for dependency management             |
| 1     | check-lockfile-admin | check-lockfile   | Update uv.lock (main branch)   | update-uv-lock-admin.yml      | workflow_dispatch                                                                                    | Admin: Updates uv.lock on main branch with PAT authentication  |
| 2     | generate-configs   | generate-configs   | Generate Config Examples       | generate_config_examples.yaml | pull_request (data_adapters/\*\*, streamer.py), workflow_dispatch                                   | Auto-generates configuration examples from source code           |
| 3     | sync-dev-to-main   | sync-dev-to-main   | Sync Dev to Main               | sync-dev-to-main.yml          | workflow_dispatch                                                                                    | Merges dev branch into main branch                                |
| 4     | sync-main-to-pt    | sync-main-to-pt    | Sync Main to Performance Testing | sync-main-to-pt.yml        | push to `main`, workflow_dispatch                                                                    | Merges main branch into feature/performance-testing branch       |
| 5     | build-and-push     | build-and-push     | Build and Push Docker Images   | docker-release.yml          | push to `main`                                                                                       | Builds newer Docker images and pushes to Docker Hub              |
| 6     | pypi-release       | publish to PyPI    | Publish Releases               | release.yml                   | push (v\* tags), workflow_dispatch                                                                   | Builds and publishes Python package to PyPI                      |
| 7     | docker-release     | build-and-push     | Publish Releases               | release.yml                   | Depends on: pypi-release, workflow_dispatch                                                          | Build newer Docker images and push to Docker Hub                 |
| 8     | github-release     | create-github-rel  | Publish Releases               | release.yml                   | Depends on: docker-release, pypi-release                                                               | Creates GitHub release with attached artifacts (tar.gz, zip)   |


## Stage Priority

- **Stage 0**: Core CI checks (installation, streaming, ROS2 tests) — runs on every push to `dev` branch or pull_request targeting it
- **Stage 1**: Dependency management (uv.lock updates) — triggers on pyproject.toml changes or manual dispatch
- **Stage 2**: Configuration examples generation — triggers when data adapter files change
- **Stage 3**: Branch synchronization (dev → main) — merges dev branch into main branch
- **Stage 4**: Branch synchronization (main → performance-testing) — merges main into feature/performance-testing branch
- **Stage 5**: Standalone Docker image building and pushing — runs on push to `main` branch
- **Stage 6+**: Package publishing pipeline (PyPI → Docker → GitHub releases) — triggered by version tags (`v*`) or manual dispatch; jobs run sequentially with dependency tracking


## Workflow Files

| # | File                                    | Type           | Description                                              |
|---|-----------------------------------------|----------------|----------------------------------------------------------|
| 1 | [ci-checks.yml](./ci-checks.yml)        | Triggerable    | Core CI checks: installation, streaming, and ROS2 tests  |
| 2 | [update-uv-lock.yml](./update-uv-lock.yml)   | Triggerable    | Checks/updates uv.lock on pull requests                |
| 3 | [update-uv-lock-admin.yml](./update-uv-lock-admin.yml) | Triggerable | Admin: updates uv.lock on main branch with PAT       |
| 4 | [generate_config_examples.yaml](./generate_config_examples.yaml) | Triggerable | Auto-generates config examples from source code    |
| 5 | [sync-dev-to-main.yml](./sync-dev-to-main.yml)      | Triggerable    | Merges dev branch into main branch                    |
| 6 | [sync-main-to-pt.yml](./sync-main-to-pt.yml)        | Triggerable    | Merges main into feature/performance-testing branch   |
| 7 | [docker-release.yml](./docker-release.yml)            | Triggerable    | Standalone Docker image build and push                |
| 8 | [release.yml](./release.yml)                      | Triggerable    | Full release pipeline: PyPI → Docker → GitHub release |
| 9 | [ros2.yaml](./ros2.yaml)                       | Reusable       | ROS2 tests (referenced by ci-checks.yml via `workflow_call`) |


## Notes

- The `ros2-check` job in `ci-checks.yml` uses a reusable workflow reference (`./.github/workflows/ros2.yaml`)
- Admin workflows use personal access tokens (PAT) for elevated permissions on protected branches
- Release workflows use tag-based triggers (`v*`) and run jobs sequentially: PyPI → Docker → GitHub release
- Jobs within `release.yml` have dependency relationships that enforce execution order