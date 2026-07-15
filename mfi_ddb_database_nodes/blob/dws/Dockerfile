FROM python:3.12-slim

WORKDIR /app

# 1. Install build dependencies, compile what's needed, then immediately clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy only requirements first to maximize layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN opentelemetry-bootstrap -a install

# 3. Copy the rest of your self-contained DWS node folder (server.py, proto/, gen/, etc.)
COPY . .

# 4. Expose the assigned gRPC port
EXPOSE 50053

# 5. Run the gRPC server directly from the root of /app
CMD ["python", "server.py"]