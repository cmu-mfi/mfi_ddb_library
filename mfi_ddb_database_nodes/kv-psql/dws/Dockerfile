FROM python:3.12-slim

WORKDIR /app

# 1. Install build dependencies and immediately clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy only requirements first to optimize layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the localized DWS node files (server.py, init_db.py, proto/, gen/, etc.)
COPY . .

# 4. Expose the internal gRPC execution port
EXPOSE 50051

# 5. Clean default runtime instruction (can be overridden by Compose command)
CMD ["python", "server.py"]