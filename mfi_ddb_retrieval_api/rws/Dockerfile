FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 1. Copy only requirements first to leverage Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt numpy

# 2. Copy the localized RWS node files (main.py, app/, schema/, services/, utils/, etc.)
COPY . .

# 3. Expose the API port
EXPOSE 8000

# 4. Since everything is sitting perfectly in the root of /app, 
# relative paths like "./app/docs/..." resolve naturally.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]