# Getting Started With MFI DDB

## Prerequisites

1. Docker Engine
   - Download and install Docker using the instructions here: [Docker Installation](https://docs.docker.com/desktop/?_gl=1*vyg1ud*_gcl_au*NjIxOTgxMjcwLjE3ODE1MzQ2NzQ.*_ga*MTc2NzQzNDQxMC4xNzczNTE0NDY5*_ga_XJWPQMJYHQ*czE3ODYwNDI1MjckbzM1JGcxJHQxNzg2MDQyNTMzJGo1NCRsMCRoMA..)

## Setup Steps

1. Go to the `docker` folder.
2. Each subfolder contains a config YAML file for its corresponding service. These files come pre-populated with default values that work out of the box.
3. To change a config, open the relevant file and edit the parameters. Each parameter has a comment above it explaining its purpose and expected value.
4. Open a terminal in the `docker` folder.
5. Decide which services you want to run, and confirm their prerequisites are installed on your machine. Using the Data Adapter App is recommended for connecting the broker to your data generator.
6. If you don't have a data generator, use `mock-publisher` service to simulate one. Information is given ahead in steps.
7. Start the services you selected:

   **All services:**
   ```
   docker compose --profile '*' up -d
   ```

   **Specific services:**
   ```
   docker compose --profile '<profile-name-1>' --profile '<profile-name-2>' up -d
   ```

8. Run `docker ps` to confirm all services are up and healthy. Confirm that `mock-publisher` and `dev-tools` are running before testing a sample flow.

## Setting Up a Connection (Data Adapter App)

1. Open the Data Adapter App and create a new connection between your data generator and the broker.
2. Select the Data Adapter that matches your data generator.
3. Fill in the **Adapter Config** and **Streamer Config** fields. Click the `(?)` icon next to any field for a description and expected input format.
   - Invalid configs are not validated automatically and double-check the format before saving.
   - For testing, you can use `test.mosquitto.org` as the broker address with port `1883`.

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
   ```

4. Click **Save**. Once initialized, the connection will show as connected and streaming on screen.

## Retrieving and Visualizing Data

Once a connection is established, you can retrieve and visualize data using Grafana Infinity.

1. Open Grafana on port `3005`.
2. Go to the **Retrieval Web Service** dashboard.
3. Use the dashboard's visualizations to view how your data changes over a selected time period.
4. Filter by user ID to check data for a specific user.