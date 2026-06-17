FROM python:3.12-slim

WORKDIR /app

# Ensure absolute monorepo imports remain functional
ENV PYTHONPATH=/app:/app/src

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Assuming there is a shared or local requirements file; adjust path if needed
COPY src/mfi_ddb/databases/blob/connector/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Run the connector script, passing the configuration file as an argument
CMD ["python", "src/mfi_ddb/databases/blob/connector/connector.py", "src/mfi_ddb/databases/blob/connector/config.yaml"]