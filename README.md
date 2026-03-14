# 💳 Electronic Payment Gateway Pipeline

A fully containerised data pipeline that generates realistic payment transaction data, streams it through Apache Kafka, and archives it as columnar Parquet files in MinIO object storage.

```
MySQL  ──►  Kafka  ──►  MinIO (Parquet)
  ▲
  │
generate_data.py  +  stream_data.py
```

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        Docker Network                           │
│                                                                 │
│  ┌──────────────┐    ┌─────────────┐    ┌──────────────────┐   │
│  │  MySQL 8.0   │    │  Zookeeper  │    │     MinIO        │   │
│  │  payment_    │    │  :2181      │    │  :9001 (UI)      │   │
│  │  gateway     │    └──────┬──────┘    │  :9002 (API)     │   │
│  │  :3306       │           │           └────────▲─────────┘   │
│  └──────┬───────┘    ┌──────▼──────┐            │             │
│         │            │   Kafka     │    ┌────────┴─────────┐   │
│  ┌──────▼───────┐    │   :9092     │    │  kafka_consumer  │   │
│  │ data_seeder  │    └──────┬──────┘    │  Kafka → Parquet │   │
│  │ (runs once)  │           │           └──────────────────┘   │
│  └──────────────┘    ┌──────▼──────┐                           │
│  ┌──────────────┐    │kafka_producer│                          │
│  │data_streamer │    │MySQL → Kafka │                          │
│  │ 2 txns/sec   │    └─────────────┘                          │
│  └──────────────┘                                              │
└─────────────────────────────────────────────────────────────────┘
```

| Service | Image | Port | Purpose |
|---|---|---|---|
| MySQL | `mysql:8.0` | `3306` | Primary transactional database |
| Adminer | `adminer:4.8.1` | `8081` | Web SQL browser |
| Zookeeper | `cp-zookeeper:7.4.1` | `2181` | Kafka coordination |
| Kafka | `cp-kafka:7.4.1` | `9092` | Message broker |
| Kafdrop | `kafdrop:latest` | `9000` | Kafka topic browser |
| MinIO | `minio:latest` | `9001/9002` | S3-compatible object storage |
| data_seeder | custom | — | One-shot bulk data generator |
| data_streamer | custom | — | Continuous live data generator |
| kafka_producer | custom | — | MySQL → Kafka CDC extractor |
| kafka_consumer | custom | — | Kafka → MinIO Parquet writer |
| Airflow | `airflow:2.9.3` | `8082` | DAG orchestration |
| PostgreSQL | `postgres:15` | `5432` | Airflow metadata store |

---

## ✅ Prerequisites

| Tool | Minimum Version | Check |
|---|---|---|
| Git | 2.30+ | `git --version` |
| Docker Engine | 24.0+ | `docker --version` |
| Docker Compose | 2.20+ | `docker compose version` |

**System resources:** 8 GB RAM minimum (16 GB recommended), 10 GB free disk.

> **Docker Desktop** (macOS / Windows) includes both Docker Engine and Compose.  
> On Linux, install them separately from [docs.docker.com](https://docs.docker.com).

---

## 🚀 Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_ORG/payment-gateway-pipeline.git
cd payment-gateway-pipeline
```

### 2. Create the environment file

```bash
cp .env.example .env
```

Open `.env` and fill in your values:

```env
# MinIO
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin123

# PostgreSQL (Airflow metadata)
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow123
POSTGRES_DB=airflow

# Airflow admin account
AIRFLOW_ADMIN_USER=admin
AIRFLOW_ADMIN_FIRSTNAME=Admin
AIRFLOW_ADMIN_LASTNAME=User
AIRFLOW_ADMIN_EMAIL=admin@example.com
AIRFLOW_ADMIN_PASSWORD=admin123
```

> ⚠️ Never commit `.env` to Git — it is already in `.gitignore`.

### 3. Create Airflow directories

```bash
mkdir -p dags logs plugins
```

> **Linux only:** `echo "AIRFLOW_UID=$(id -u)" >> .env`

### 4. Build and start the stack

```bash
docker compose pull      # download base images (~4-6 GB)
docker compose build     # build custom Python images
docker compose up -d     # start everything
```

### 5. Watch the seeder (takes 3–8 minutes)

```bash
docker compose logs -f payment_data_seeder
```

When you see `✅ DATA GENERATION COMPLETE` the full stack is ready.

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---|---|---|
| Adminer (MySQL UI) | http://localhost:8081 | Server: `mysql` · User: `pg_user` · Pass: `pg_password_2024` · DB: `payment_gateway` |
| Kafdrop (Kafka UI) | http://localhost:9000 | None required |
| MinIO Console | http://localhost:9001 | User: `minioadmin` · Pass: from `.env` |
| Airflow UI | http://localhost:8082 | User: `admin` · Pass: from `.env` |

---

## 📁 Project Structure

```
.
├── docker-compose.yml              # Full stack orchestration
├── .env.example                    # Environment variable template
├── init-scripts/
│   └── init.sql                    # MySQL schema + reference data
│
├── Data-Sources/
│   ├── Dockerfile                  # Python image for seeder/streamer
│   ├── entrypoint.sh               # MySQL readiness wait script
│   ├── generate_data.py            # Bulk historical data generator
│   └── stream_data.py              # Real-time streaming generator
│
└── Pipeline/
    ├── Dockerfile.pipeline         # Python image for Kafka services
    ├── entrypoint_pipeline.py      # Pure-Python service readiness entrypoint
    ├── wait_for.py                 # TCP / schema readiness checker
    ├── kafka_producer.py           # MySQL → Kafka (full load + CDC)
    └── kafka_consumer.py           # Kafka → MinIO (Parquet files)
```

---

## 🗄️ Database Schema

The `payment_gateway` MySQL database contains **13 tables** covering the full payment lifecycle:

```
countries  card_networks  transaction_types  transaction_statuses  merchant_categories
     │                                                │
  merchants ──────────────────────────────── settlement_batches
     │
  pos_terminals
                    clients ──── payment_cards
                       │               │
                       └──── transactions ──── fraud_alerts
                                    │
                                 refunds
```

**Seeded data volumes:**

| Entity | Volume |
|---|---|
| Merchants | 200 |
| POS Terminals | 1,000 |
| Clients | 10,000 |
| Payment Cards | ~18,000 |
| Transactions | 155,000+ (1 year of history) |
| Fraud Alerts | ~31,000 |
| Settlement Batches | 73,000 (200 merchants × 365 days) |

---

## 📡 Kafka Topics

The producer creates **13 topics** (one per MySQL table), all prefixed with `pg.`:

```
pg.transactions       pg.clients          pg.merchants
pg.pos_terminals      pg.payment_cards    pg.fraud_alerts
pg.refunds            pg.settlement_batches
pg.countries          pg.card_networks    pg.transaction_types
pg.transaction_statuses                   pg.merchant_categories
```

Each message format:

```json
{
  "_meta": { "table": "transactions", "op": "c", "ts_ms": 1712345678000 },
  "payload": { "txn_id": 1042, "amount": 142.50, "currency": "USD", ... }
}
```

The producer runs in two phases:
1. **Full Load** — reads all historical rows on startup (skipped if topics already have data)
2. **CDC Poll** — queries for new rows every 15 seconds using a `created_at` watermark

---

## 🪣 MinIO / Parquet Output

The consumer writes **Snappy-compressed Parquet** files using Hive-style partitioning:

```
s3://payment-gateway/
  raw/
    transactions/
      year=2025/month=03/day=14/
        20250314_153022_abc123.parquet   ← 1,000 rows, ~180 KB
    clients/
      year=2025/month=03/day=14/
        ...
```

Files are flushed when **either** condition is met:
- Buffer reaches **1,000 rows**
- **30 seconds** have elapsed since the last flush

**Reading the files:**

```python
# Pandas
import pandas as pd
df = pd.read_parquet(
    "s3://payment-gateway/raw/transactions/year=2025/month=03/day=14/",
    storage_options={"endpoint_url": "http://localhost:9002",
                     "key": "minioadmin", "secret": "minioadmin123"}
)

# DuckDB
import duckdb
duckdb.sql("""
    SELECT channel, COUNT(*) AS txns, SUM(amount) AS revenue
    FROM   read_parquet('s3://payment-gateway/raw/transactions/**/*.parquet')
    GROUP  BY channel
""").show()
```

---

## ⚡ Live Streaming Rates

After seeding completes, `stream_data.py` continuously inserts:

| Event | Rate |
|---|---|
| Normal transactions | 2 / second |
| Fraud campaign (random pattern) | 1 / hour |
| New clients | 2 / hour |
| New POS terminals | 1 / day |

**Fraud patterns** (one randomly selected per campaign):
- 💨 **Velocity Burst** — same client, 5–20 rapid transactions
- 🔍 **Card Testing** — micro-charges $0.01–$1.99
- 💸 **High-Value Fraud** — single transaction $3k–$10k
- 🌙 **Night Owl** — transactions between 01:00–04:59
- 🌍 **International Spree** — multi-currency rapid succession
- 🔓 **Account Takeover** — new device + foreign IP + high amount

---

## 🔧 Configuration

All tuneable parameters are set via environment variables in `.env` or in `docker-compose.yml`:

| Variable | Default | Description |
|---|---|---|
| `POLL_INTERVAL` | `15` | Seconds between CDC polls |
| `BATCH_SIZE` | `500` | Rows per Kafka send batch |
| `FLUSH_ROWS` | `1000` | Row threshold to write Parquet |
| `FLUSH_SECONDS` | `30` | Time threshold to write Parquet |
| `PARQUET_COMPRESSION` | `snappy` | Codec: `snappy` / `gzip` / `zstd` / `none` |

---

## 🛠️ Common Commands

```bash
# Start
docker compose up -d

# Stop (data preserved)
docker compose down

# Full reset — wipe all data
docker compose down -v && docker compose up --build -d

# Live logs
docker compose logs -f kafka_producer
docker compose logs -f kafka_consumer
docker compose logs -f payment_data_streamer

# MySQL shell
docker exec -it payment_gateway_db \
  mysql -u pg_user -ppg_password_2024 payment_gateway

# Check consumer lag
docker exec -it kafka \
  kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group pg-minio-consumer

# Update after git pull
git pull origin main
docker compose up --build -d
```

---

## ❓ Troubleshooting

| Symptom | Fix |
|---|---|
| `payment_data_seeder` exits with code 1 | `docker logs payment_data_seeder` — if schema empty: `docker compose down -v && docker compose up --build` |
| `[Kafka] Attempt N/90` in pipeline logs | Check `docker compose ps kafka zookeeper` — if unhealthy: `docker compose restart zookeeper kafka` |
| No topics in Kafdrop | Wait for seeder to finish (`exited (0)`), then check `docker logs kafka_producer` |
| No Parquet files in MinIO | Check `docker logs kafka_consumer` and verify MinIO is healthy |
| Port already in use | `lsof -i :PORT` (macOS/Linux) — stop the conflicting process or change the port in `docker-compose.yml` |
| Airflow not reachable | Wait 2–3 min after first boot — `docker logs airflow-init` to check progress |

---

## 📄 Documentation

| Document | Description |
|---|---|
| `docs/pipeline_documentation.docx` | Full technical reference — schema, data generation, Kafka producer/consumer, Parquet format |
| `docs/deployment_guide.docx` | Step-by-step deployment guide with troubleshooting |

---

## 📦 Tech Stack

![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.4-231F20?logo=apachekafka&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49?logo=minio&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE?logo=apacheairflow&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-PyArrow-50ABF1?logo=apacheparquet&logoColor=white)
