FROM python:3.12-slim

WORKDIR /app

ENV PYTHONPATH=/app:/app/src:/app/src/mfi_ddb/databases/blob/dws

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY src/mfi_ddb/databases/blob/dws/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Expose the standard gRPC port assigned to this service (e.g., 50053)
EXPOSE 50053

WORKDIR /app/src/mfi_ddb/databases/blob/dws

CMD ["python", "server.py"]