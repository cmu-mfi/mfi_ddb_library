FROM python:3.12-slim

WORKDIR /app

# Install native C optimization extensions required for compiling/linking database drivers
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 1. Copy requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2. Copy the localized directory files (.py files, .ini configs, etc.)
COPY . .

ENV PYTHONUNBUFFERED=1

# Run connector.py passing the localized configuration files
CMD ["python", "-u", "connector.py", "--broker_config", "broker.ini", "--pg_config", "pg_database.ini"]