FROM ubuntu:20.04
ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y \
    python3 python3-pip curl git build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN pip install uv

WORKDIR /app

# Now requirements.txt is in the context
COPY requirements.txt .

RUN uv pip install --system -r requirements.txt

# Copy only your backend code
COPY web_interface/data_adapter/backend/app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
