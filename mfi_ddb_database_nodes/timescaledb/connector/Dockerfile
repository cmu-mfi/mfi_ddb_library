FROM python:3.12-slim

WORKDIR /app

# 1. Install lean build dependencies and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the localized connector files (main.py, db.py, config.yaml, etc.)
COPY . .

# 4. Retain unbuffered logs configuration for neat streaming profiles
ENV PYTHONUNBUFFERED=1

# 5. Execute the connector script directly from the local workspace root
CMD ["python", "-u", "main.py"]