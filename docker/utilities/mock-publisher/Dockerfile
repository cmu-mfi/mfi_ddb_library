FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir paho-mqtt pyyaml

COPY mqttmock.py .
COPY config.yaml .

CMD ["python", "mqttmock.py"]