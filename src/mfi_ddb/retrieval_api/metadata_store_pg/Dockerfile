FROM python:3.12-slim

WORKDIR /app

# Install native C optimization extensions required for compiling/linking database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 1. Install specific, isolated directory modules
COPY src/mfi_ddb/retrieval_api/metadata_store_pg/requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the top-level src tree to maintain cross-cutting package imports
COPY src/ /app/src/

# 3. Mount runtime working parameters to local context
COPY src/mfi_ddb/retrieval_api/metadata_store_pg/*.py /app/
COPY src/mfi_ddb/retrieval_api/metadata_store_pg/*.ini /app/

ENV PYTHONPATH=/app/src:/app
ENV PYTHONUNBUFFERED=1

# Run connector.py (which runs internal check_tables and create_tables on startup)
CMD ["python", "-u", "connector.py", "--broker_config", "broker.ini", "--pg_config", "pg_database.ini"]



# OLD DOCKERFILE

# FROM python:3.12-slim

# RUN apt update && apt install -y git

# COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# RUN git clone --recurse-submodules https://github.com/cmu-mfi/mfi_ddb_library.git && \
#     cd mfi_ddb_library && \
#     uv pip install --system .