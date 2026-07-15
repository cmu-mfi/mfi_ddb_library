FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Since Dockerfile is inside /backend, requirements.txt is right next to it!
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN opentelemetry-bootstrap -a install

# Copies everything inside /backend straight into /app
COPY . .

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]