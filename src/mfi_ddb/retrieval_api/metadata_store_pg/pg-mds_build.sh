#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# 1. Define the port variable (defaults to 5432 if DB_PORT is not set in the environment)
TARGET_PORT="${DB_PORT:-5432}"
DB_USER="mfi"
DB_PASS="mfiddb"
DB_NAME="mds"

echo "Cleaning up old containers..."
docker stop mds_postgres 2>/dev/null || true
docker rm mds_postgres 2>/dev/null || true

echo "Starting PostgreSQL container on port ${TARGET_PORT}..."
# 2. Spin up a clean PostgreSQL container matching your pg_database.ini configuration
docker run -d --name mds_postgres \
  -p "${TARGET_PORT}":5432 \
  -e POSTGRES_USER="${DB_USER}" \
  -e POSTGRES_PASSWORD="${DB_PASS}" \
  -e POSTGRES_DB="${DB_NAME}" \
  postgres:15-alpine

echo "Waiting for PostgreSQL to become completely ready..."
until docker exec mds_postgres pg_isready -U "${DB_USER}" -d "${DB_NAME}"; do
  echo "Database is still initializing... waiting 2 seconds"
  sleep 2
done

echo "Database is up! Bootstrapping schemas and tables..."

# 3. Use your existing Python ecosystem (uv) to create and verify the tables.
# This eliminates needing a local psql client installation on your host engine.
if command -v uv &> /dev/null; then
    echo "Running schema generation via uv..."
    uv run pg_create_tables.py --pg_config pg_database.ini
    uv run pg_check_tables.py --pg_config pg_database.ini
else
    echo "uv not found, running schema generation via python..."
    python pg_create_tables.py --pg_config pg_database.ini
    python pg_check_tables.py --pg_config pg_database.ini
fi

echo "PostgreSQL container build and table setups completed successfully on port ${TARGET_PORT}! 🚀"