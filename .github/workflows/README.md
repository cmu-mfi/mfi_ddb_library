# CI Workflows

This repository uses GitHub Actions for continuous integration and automation. Below are the available workflows organized by priority/stage.

## workflows


| Stage | Job ID             | Job name           | Workflow name                  | Workflow file                 | Events                                                                                               | Description                                                      |
|-------|--------------------|--------------------|--------------------------------|-------------------------------|------------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| 0     | installation-check | installation-check | MFI-DDB Package CI Checks      | ci-checks.yml                 | push to main, pull_request (mfi_ddb_package/src/\*\*)                                                | Verifies installation setup across Ubuntu and Windows            |
| 0     | streaming-check    | streaming-check    | MFI-DDB Package CI Checks      | ci-checks.yml                 | push to main, pull_request (mfi_ddb_package/src/\*\*)                                                | Tests data adapters (LocalFiles, MQTT, Kv) with EMQX broker     |
| 0     | ros2-check         | ros2-check         | MFI-DDB Package CI Checks      | ci-checks.yml                 | push to main, pull_request (mfi_ddb_package/src/\*\*)                                                | Runs ROS2 tests                                                  |
| 1     | check-lockfile     | check-lockfile     | Update uv.lock                 | update-uv-lock.yml            | pull_request (pyproject.toml), workflow_dispatch                                                    | Checks and updates uv.lock for dependency management             |
| 1     | check-lockfile-admin | check-lockfile   | Update uv.lock (main branch)   | update-uv-lock-admin.yml      | workflow_dispatch                                                                                    | Admin: Updates uv.lock on main branch with PAT authentication  |
| 2     | generate-configs   | generate-configs   | Generate Config Examples       | generate_config_examples.yaml | pull_request (data_adapters/\*\*, streamer.py), workflow_dispatch                                   | Auto-generates configuration examples from source code           |
| 3     | sync-dev-to-main   | sync-dev-to-main   | Sync Dev to Main               | sync-dev-to-main.yml          | workflow_dispatch                                                                                    | Merges dev branch into main branch                                |
| 4     | sync-main-to-pt   | sync-main-to-pt   | Sync feature/performance-testing with Main               | sync-main-to-pt.yml          | push to main, workflow_dispatch                                                                                    | Merges main branch into feature/performance-testing branch                                |
| 5     | build-and-push   | build-and-push   | Build and Push Docker Images               | docker-release.yml          | push to main                                                                                    | Build newer docker images and push to Docker Hub                                |


## Stage Priority

- **Stage 0**: Core CI checks (installation, streaming, ROS2 tests) - runs on every push/pull_request to main
- **Stage 1**: Dependency management (uv.lock updates) - triggers on pyproject.toml changes or manual dispatch
- **Stage 2**: Configuration examples generation - triggers when data adapter files change

## Notes

- The `ros2-check` job uses a reusable workflow reference (`/mfi_ddb_package/tests/workflows/data_adapters/ros2.yaml`)
- Admin workflows use personal access tokens (PAT) for elevated permissions on protected branches
