FROM python:3.12-slim

WORKDIR /app

# 1. Install lean compilation packages and clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    build-essential \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy only requirements first to maximize layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN opentelemetry-bootstrap -a install

# 3. Copy the localized DWS files (server.py, db.py, proto/, gen/, etc.)
COPY . .

# 4. Expose the assigned gRPC port
EXPOSE 50052

# 5. Execute the server script directly from the root of /app
CMD ["python", "server.py"]