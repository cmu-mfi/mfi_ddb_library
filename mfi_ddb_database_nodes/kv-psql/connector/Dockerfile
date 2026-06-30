FROM python:3.12-slim

WORKDIR /app

# 1. Install lean build dependencies and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the localized connector code (connector.py, config.yaml, etc.)
COPY . .

# 4. Execute the connector module directly from the local workspace
CMD ["python", "connector.py"]