FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Data-Sources/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Data-Sources/generate_data.py .
COPY Data-Sources/stream_data.py   .
COPY Data-Sources/entrypoint.py    .

# Pure Python entrypoint — no shell required
ENTRYPOINT ["python3", "/app/entrypoint.py"]
