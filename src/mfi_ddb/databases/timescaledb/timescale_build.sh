docker run -d --name timescaledb \
  -p 5432:5432 \
  -e POSTGRES_USER=tsdb \
  -e POSTGRES_PASSWORD=timescale \
  -e POSTGRES_DB=ddb_ts \
  timescale/timescaledb:latest-pg15

sleep 5

psql -h localhost -U tsdb -d ddb_ts <<EOF
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
EOF