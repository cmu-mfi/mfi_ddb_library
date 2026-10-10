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

> [!NOTE]
> * Each docker subfolder contains a config YAML file for its corresponding service.
> * Available profiles: `*`, `dbn`, `retrieval`, `mqtt-broker`, `daa`, `dev-tools`. [More info here](./docker/README.md) <!--(https://github.com/cmu-mfi/mfi_ddb_library/tree/main/docker)-->

## Setting Up a Connection (Data Adapter App)

1. Open the Data Adapter App at [http://localhost:3001](http://localhost:3001).
<img width="820" height="866" alt="Screenshot 2026-08-06 165426" src="https://github.com/user-attachments/assets/03b94f8b-b6dd-4786-8b9a-691d9a123036" />

2. Click on `+ New Adapter` and select `MQTT` data adapter
<img width="820" height="866" alt="Screenshot 2026-08-06 165601" src="https://github.com/user-attachments/assets/566fd065-7ab7-4ee5-8252-dd657f9affe9" />

3. We are going to use a mock mqtt publisher, publishing data to a public broker (https://test.mosquitto.org/). The publisher is one of the docker services running in the background if you chose `*` profile. Fill in the **Adapter Config** and **Streamer Config** fields.

> [!TIP]
> The trial_id is to group a set of data for a specific "trial". If running experiments or processes repeatedly, and each run needs to be tagged separately, change this `trial_id` each time. Feel free to add new key/value pairs for additional metadata info.

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
     enterprise: CMU
     site: Test-Site
   project:
     project_name: Test Trial Project
   ```

<img width="820" height="866" alt="Screenshot 2026-08-06 170903" src="https://github.com/user-attachments/assets/e168e7a9-28e3-482d-98a6-448b54c57c6d" />

4. Click **Save**. Once initialized, the connection will show as connected and streaming on screen.
<img width="820" height="866" alt="Screenshot 2026-08-06 173329" src="https://github.com/user-attachments/assets/c71d2ada-80b4-4017-9552-4dffac962c35" />


## Retrieving and Visualizing Data

Once a connection is established, you can retrieve and visualize data using Grafana Infinity.

1. Open Grafana: [http://localhost:3005](http://localhost:3005).
2. Use `admin`/`admin` for username password.
3. Dashboards --> RWS Infinity
<img width="1269" height="922" alt="Screenshot 2026-08-06 193652" src="https://github.com/user-attachments/assets/0440dbe1-1130-4874-ad6e-ed0c8a8911b2" />

4. Use the dashboard's visualizations to view how your data changes over a selected time period.
5. Filter by trial ID to check data for a specific trial.
<img width="2560" height="1392" alt="Screenshot 2026-08-06 194740" src="https://github.com/user-attachments/assets/d7598703-4c40-4162-8155-fd2003e44be4" />
