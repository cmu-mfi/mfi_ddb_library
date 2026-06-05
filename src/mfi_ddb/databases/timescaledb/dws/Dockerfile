# Use a lightweight Python base image
FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy and install DWS service requirements
COPY src/mfi_ddb/databases/timescaledb/dws/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the source directory for clean package resolution
COPY src/ /app/src/

ENV PYTHONPATH=/app/src

# Expose the internal gRPC port
EXPOSE 50052

# Run the gRPC server module
CMD ["python", "-m", "mfi_ddb.databases.timescaledb.dws.server"]