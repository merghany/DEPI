FROM python:3.11-slim

# System deps for mysql-connector
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python packages
RUN pip install --no-cache-dir \
    mysql-connector-python \
    faker \
    tqdm

# Copy scripts and entrypoint
COPY Data-Sources/generate_data.py .
COPY Data-Sources/stream_data.py   .
COPY Data-Sources/entrypoint.sh    .

RUN chmod +x entrypoint.sh

ENTRYPOINT ["./entrypoint.sh"]
