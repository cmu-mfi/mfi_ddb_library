FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from the context path
COPY src/mfi_ddb/databases/kv-psql/connector/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire source tree for absolute import mappings
COPY src/ /app/src/

ENV PYTHONPATH=/app/src

# Execute the connector module explicitly
CMD ["python", "-m", "mfi_ddb.databases.kv-psql.connector.connector"]