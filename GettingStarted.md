# Getting Started With MFI DDB

## Prerequisites

1. Docker Engine
   - Download and install Docker using the instructions here: [Docker Installation](https://docs.docker.com/desktop/?_gl=1*vyg1ud*_gcl_au*NjIxOTgxMjcwLjE3ODE1MzQ2NzQ.*_ga*MTc2NzQzNDQxMC4xNzczNTE0NDY5*_ga_XJWPQMJYHQ*czE3ODYwNDI1MjckbzM1JGcxJHQxNzg2MDQyNTMzJGo1NCRsMCRoMA..)

2. Default ports are available. If you see any ports `IN USE`, make sure to edit the config files or terminate the existing process using it.

```
$ cd docker

# Linux / MacOS
$ bash ports-check.sh

# Windows Powershell
$ ./ports-check.ps1

ALL OK!
```

## Setup Steps

1. Go to the `docker` folder.

    ```
    cd docker 
    ```

2. Start all services:

   **All services:**
   ```
   docker compose --profile '*' up -d
   ```

   **(advanced users) Specific services:**
   ```
   docker compose --profile '<profile-name-1>' --profile '<profile-name-2>' ... up -d
   ```

3. Run `docker ps` to confirm all services are up and healthy. Confirm that `mock-publisher` and `dev-tools` are running before testing a sample flow.

> ![Note]
> * Each docker subfolder contains a config YAML file for its corresponding service.
> * Available profiles: `*`, `dbn`, `retrieval`, `mqtt-broker`, `daa`, `dev-tools`. [More info here](./docker/README.md) <!--(https://github.com/cmu-mfi/mfi_ddb_library/tree/main/docker)-->

## Setting Up a Connection (Data Adapter App)

1. Open the Data Adapter App at [http://localhost:3001](http://localhost:3001).

<!-- insert-screenshot -->

2. Click on `+ New Adapter` and select `MQTT` data adapter

<!-- insert-screenshot -->

3. We are going to use a mock mqtt publisher, publishing data to a public broker (https://test.mosquitto.org/). The publisher is one of the docker services running in the background if you chose `*` profile. Fill in the **Adapter Config** and **Streamer Config** fields.

   **Adapter Config**
   ```yaml
   mqtt:
     broker_address: test.mosquitto.org
     broker_port: 1883
   trial_id: test_trial_001
   queue_size: 10
   topics:
     - component_id: telemetry
       topic: admin/feeds/avroom/telemetry/#
   ```

   **Streamer Config**
   ```yaml
   user:
     user_id: test_user
     domain: default
   mqtt:
     broker_address: localhost
     broker_port: 1883
     enterprise: test_ent
     site: Test-Site
   project:
     project_name: Test Trial Project
   ```

<!-- insert image -->

4. Click **Save**. Once initialized, the connection will show as connected and streaming on screen.

<!-- insert screenshot -->

## Retrieving and Visualizing Data

Once a connection is established, you can retrieve and visualize data using Grafana Infinity.

1. Open Grafana on port `3005`.
2. Go to the **Retrieval Web Service** dashboard.
3. Use the dashboard's visualizations to view how your data changes over a selected time period.
4. Filter by user ID to check data for a specific user.