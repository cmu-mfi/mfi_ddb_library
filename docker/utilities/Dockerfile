FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt pyyaml

COPY publisher.py .
COPY config.yaml .

CMD ["python", "publisher.py"]