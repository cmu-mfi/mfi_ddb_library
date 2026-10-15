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