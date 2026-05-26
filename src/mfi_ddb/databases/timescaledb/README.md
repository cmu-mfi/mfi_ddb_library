## TimeScaleDB Database Node

This directory implements an open-source historian replacement for Aveva PI using
TimeScaleDB. It follows the standard MFI-DDB database node pattern:

- Connector: subscribes to MQTT historian topics and writes rows to TimeScaleDB.
- Database: TimeScaleDB (PostgreSQL + time-series extension).
- DWS: gRPC service that exposes APIs for retrieval (GetDataPoint/GetDataRange/StreamData(using polling)).

### Folder structure

```
timescaledb/
	connector/
		main.py        # MQTT -> Sparkplug B decode -> TimeScale insert
		db.py          # TimeScale writer helper
		config.yaml    # Connector config
		requirements.txt
	dws/
		server.py      # DWS gRPC server
		db.py          # TimeScale reader helper
		config.yaml    # DWS config
		requirements.txt
	tests/
		test_dws.py
		test_connector.py
	timscale_build.sh # Docker startup helper
```

### Prerequisites

- Docker
- psql client
- Python virtualenv for running the connector/DWS

### Start TimeScaleDB (Docker)

```
bash timescale_build.sh
```

This script starts a TimeScaleDB container. 

If your script does not create the schema, run the SQL below manually in psql.

### Schema (run once)

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS timeseries_data (
	time        TIMESTAMPTZ NOT NULL,
	topic       TEXT NOT NULL,
	component   TEXT NOT NULL,
	metric      TEXT NOT NULL,
	value_num   DOUBLE PRECISION,
	value_text  TEXT,
	value_json  JSONB
);

SELECT create_hypertable('timeseries_data', 'time', if_not_exists => TRUE);
```

### Connector

The connector listens to historian topics (`mfi-v1.0-historian/#`), decodes
Sparkplug B payloads, and writes rows into TimeScaleDB.

1) Update [connector/config.yaml](connector/config.yaml) for your broker and DB.
2) Install dependencies:

```
pip install -r connector/requirements.txt
```

3) Run:

```
python connector/main.py
```

### MQTT Broker (Docker)

Start a local Mosquitto broker:

```
docker run -d --name mqtt -p 1883:1883 eclipse-mosquitto:2
```

### Streamer (Sparkplug historian source)

The connector expects Sparkplug historian messages on `mfi-v1.0-historian/#`.
You can generate these using the built-in streamer + MQTT adapter.

1) Update [examples/configs/streamer/streamer.yaml](../../../../examples/configs/streamer/streamer.yaml):
	 - Set `topic_family: historian`
	 - Set broker fields to `localhost` and your `enterprise` + `site`
2) Update [examples/configs/data_adapters/mqtt.yaml](../../../../examples/configs/data_adapters/mqtt.yaml):
	 - Set `mqtt.broker_address: localhost`
	 - Use a single topic (example below)

```
topics:
- component_id: demo-component
	topic: demo/data
	trial_id: trial_001
```

3) Run the streamer:

```
python -m mfi_ddb.scripts.stream_data -d "MQTT" \
	--adapter_cfg examples/configs/data_adapters/mqtt.yaml \
	--streamer_cfg examples/configs/streamer/streamer.yaml
```

4) Publish a test value:

```
docker exec -i mqtt mosquitto_pub -h localhost -t demo/data -m '{"temperature": 21.5}'
```

### DWS gRPC Service

The DWS server exposes the standard database node contract for retrieval.

1) Update [dws/config.yaml](dws/config.yaml).
2) Install dependencies:

```
pip install -r dws/requirements.txt
```

3) Run:

```
python dws/server.py
```

If you run as a module from the repo root, use:

```
PYTHONPATH=src python -m mfi_ddb.databases.timescaledb.dws.server
```

### DWS Test Client

Run the smoke client (GetDataPoint/GetDataRange are commented out; StreamData is looped):

```
python dws/test_client.py
```

To see streaming updates, publish additional values via MQTT.

### Verify inserts (psql)

```
PGPASSWORD=timescale psql -h localhost -U tsdb -d ddb_ts \
	-c "select time, topic, metric, value_num, value_text from timeseries_data order by time desc limit 10;"
```

### Testing (basic)

Tests are placeholders. You can extend them once MQTT and TimeScale are running.

```
pytest tests/
```
