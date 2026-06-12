# Use a lightweight Python base image
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies if required
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker layer caching
COPY src/mfi_ddb/databases/timescaledb/connector/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire source tree so internal relative package imports resolve perfectly
COPY src/ /app/src/

# FIX: Added the nested timescaledb directory to the PYTHONPATH
ENV PYTHONPATH=/app/src:/app/src/mfi_ddb/databases/timescaledb
ENV PYTHONUNBUFFERED=1

# Run the ingestion client module with unbuffered output
CMD ["python", "-u", "-m", "mfi_ddb.databases.timescaledb.connector.main"]