# Use a lightweight Python base image
FROM python:3.12-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies if required (psycopg2-binary usually doesn't need libpq-dev, but good for safety)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first to leverage Docker layer caching
COPY src/mfi_ddb/databases/timescaledb/connector/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire source tree so internal relative package imports resolve perfectly
COPY src/ /app/src/

# Set the PYTHONPATH environment variable so python can locate the modules
ENV PYTHONPATH=/app/src

# Run the ingestion client module
CMD ["python", "-m", "mfi_ddb.databases.timescaledb.connector.main"]