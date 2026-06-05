# Use a lightweight Python base image
FROM python:3.12-slim

WORKDIR /app

# Install system compilation packages for psycopg2 variants if needed
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from the repository layout context path
COPY src/mfi_ddb/databases/kv-psql/dws/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire source directory to satisfy top-level absolute paths
COPY src/ /app/src/

# Instruct Python where to begin finding root modules
ENV PYTHONPATH=/app/src

# Expose the internal gRPC execution port
EXPOSE 50051

# Execute the Key-Value Data Web Service gRPC engine wrapper module
CMD ["python", "-m", "mfi_ddb.databases.kv-psql.dws.server"]