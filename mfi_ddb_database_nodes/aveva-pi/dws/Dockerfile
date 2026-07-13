FROM python:3.12-slim

WORKDIR /app

# 1. Install lightweight build tools and clean package lists
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# 2. Leverage Docker layer caching for python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy source code files (server.py, piwebapi.py, proto/, gen/, etc.)
COPY . .

# 4. Expose the standard internal gRPC execution port
EXPOSE 50054

# 5. Execute the server script
CMD ["python", "server.py"]