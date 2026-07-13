FROM python:3.12-slim

WORKDIR /app

# 1. Install build dependencies, compile what's needed, then immediately clean up
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# 2. Copy only the requirements file first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 3. Copy the rest of your self-contained connector directory files
COPY . .

# 4. Since everything is sitting in the root of /app inside the container, 
# your paths are simplified down to the local directory.
CMD ["python", "connector.py", "config.yaml"]