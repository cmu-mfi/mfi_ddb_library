FROM python:3.12-slim

# 1. Set the initial working directory to pull requirements
WORKDIR /app

# Keeps your absolute cross-module imports healthy
ENV PYTHONPATH=/app:/app/src:/app/src/mfi_ddb/retrieval_api/rws

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Cache dependencies
COPY src/mfi_ddb/retrieval_api/rws/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir numpy

# Copy the entire codebase structure
COPY src/ ./src/

# 2. CRUCIAL: Shift the active execution directory straight to the rws folder.
# This forces "." relative paths like "./app/docs/..." to read accurately!
WORKDIR /app/src/mfi_ddb/retrieval_api/rws

EXPOSE 8000

# Run uvicorn directly from the new working directory
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]