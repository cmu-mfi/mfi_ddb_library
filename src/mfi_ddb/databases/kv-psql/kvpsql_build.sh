#!/bin/bash
set -e # Exit immediately if a command exits with a non-zero status

# 1. Define the port variable (defaults to 5432 if DB_PORT is not set in the environment)
TARGET_PORT="${DB_PORT:-5432}"
CONTAINER_NAME="kv-postgres"

echo "Cleaning up old containers..."
docker stop "$CONTAINER_NAME" 2>/dev/null || true
docker rm "$CONTAINER_NAME" 2>/dev/null || true

echo "Starting PostgreSQL container on port ${TARGET_PORT}..."
# 2. Use the variable in the port mapping config [HOST_PORT:CONTAINER_PORT]
docker run -d --name "$CONTAINER_NAME" \
  -p "${TARGET_PORT}":5432 \
  -e POSTGRES_USER=mfi \
  -e POSTGRES_PASSWORD=mfiddb \
  -e POSTGRES_DB=mfi_kv \
  postgres:15-alpine

echo "Waiting for PostgreSQL to become completely ready..."
until docker exec "$CONTAINER_NAME" pg_isready -U mfi -d mfi_kv; do
  echo "Database is still initializing... waiting 2 seconds"
  sleep 2
done

echo "Database is up! Executing schema setup via init_db.py..."

# 3. Automatically run your Python initialization script using uv or standard python3
if command -v uv &> /dev/null; then
  uv run dws/init_db.py
else
  python3 dws/init_db.py
fi

echo "PostgreSQL build and initialization completed successfully on port ${TARGET_PORT}! 🚀"