#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# 1. Define the port variable (defaults to 5432 if DB_PORT is not set in the environment)
TARGET_PORT="${DB_PORT:-5432}"

echo "Cleaning up old containers..."
docker stop timescaledb 2>/dev/null || true
docker rm timescaledb 2>/dev/null || true

echo "Starting TimescaleDB container on port ${TARGET_PORT}..."
# 2. Use the variable in the port mapping config [HOST_PORT:CONTAINER_PORT]
docker run -d --name timescaledb \
  -p "${TARGET_PORT}":5432 \
  -e POSTGRES_USER=tsdb \
  -e POSTGRES_PASSWORD=timescale \
  -e POSTGRES_DB=ddb_ts \
  timescale/timescaledb:latest-pg15

echo "Waiting for TimescaleDB to become completely ready..."
until docker exec timescaledb pg_isready -U tsdb -d ddb_ts; do
  echo "Database is still initializing... waiting 2 seconds"
  sleep 2
done

echo "Database is up! Executing schema setup..."

export PGPASSWORD="timescale"

# 3. Pass the variable to the psql host port flag (-p)
psql -h localhost -p "${TARGET_PORT}" -U tsdb -d ddb_ts <<EOF
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

echo "TimescaleDB build completed successfully on port ${TARGET_PORT}! 🚀"



# docker run -d --name timescaledb \
#   -p 5432:5432 \
#   -e POSTGRES_USER=tsdb \
#   -e POSTGRES_PASSWORD=timescale \
#   -e POSTGRES_DB=ddb_ts \
#   timescale/timescaledb:latest-pg15

# sleep 5

# psql -h localhost -U tsdb -d ddb_ts <<EOF
# CREATE EXTENSION IF NOT EXISTS timescaledb;

# CREATE TABLE IF NOT EXISTS timeseries_data (
#   time        TIMESTAMPTZ NOT NULL,
#   topic       TEXT NOT NULL,
#   component   TEXT NOT NULL,
#   metric      TEXT NOT NULL,
#   value_num   DOUBLE PRECISION,
#   value_text  TEXT,
#   value_json  JSONB
# );

# SELECT create_hypertable('timeseries_data', 'time', if_not_exists => TRUE);
# EOF



# pip package for mqtt package