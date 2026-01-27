# CI Workflows

| Stage | Job ID             | Job name           | Workflow name         | Workflow file      | Events                       | Description                  |
|-------|--------------------|--------------------|-----------------------|--------------------|------------------------------|------------------------------|
| 0     | installation-check | installation-check | CI Checks             | checks.yml         | push,pull_request            | Verifies installation setup  |
| 0     | check-lockfile     | check-lockfile     | Check and update uv.lock | update-uv-lock.yml | pull_request,workflow_dispatch | Manages uv.lock dependencies |
| 1     | streaming-check    | streaming-check    | CI Checks             | checks.yml         | push,pull_request            | Tests data adapters |
