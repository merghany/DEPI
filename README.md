# 💳 Electronic Payment Gateway Pipeline

A fully containerised end-to-end data platform: MySQL transactions captured in real time via **Debezium CDC**, streamed through **Apache Kafka**, archived as **Parquet** in **MinIO**, transformed by **dbt**, and orchestrated by **Apache Airflow**.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│  DATA GENERATION                                                    │
│  generate_data.py ──► 155k txns + 10k clients (once)               │
│  stream_data.py   ──► 2 txns/sec + 1 fraud campaign/hr             │
│                          │                                          │
│                          ▼                                          │
│  ┌────────────────────────────────┐                                 │
│  │  MySQL 8.0  (payment_gateway) │  binlog: ROW format             │
│  │  13 tables  ·  155,000+ rows  │  server-id=1                    │
│  └──────────────┬─────────────────┘                                 │
│                 │  binlog stream                                     │
│                 ▼                                                    │
│  ┌─────────────────────────────────────┐                            │
│  │  Debezium 2.4  (Kafka Connect)      │                            │
│  │  snapshot.mode = initial            │  ← full load on first run  │
│  │  then streams INSERT/UPDATE/DELETE  │  ← < 1 second latency      │
│  └──────────────┬──────────────────────┘                            │
│                 │  cdc.payment_gateway.*  (13 topics)               │
│                 ▼                                                    │
│  ┌──────────────────────────────────────────────────────┐           │
│  │  Apache Kafka 3.4                                    │           │
│  │  Kafdrop UI  :9000                                   │           │
│  └──────────────┬───────────────────────────────────────┘           │
│                 │                                                    │
│                 ▼                                                    │
│  kafka_consumer ──► Parquet (Snappy) ──► MinIO                      │
│                      Hive partitioned                                │
│                      raw/{table}/year=/month=/day/                  │
│                          │                                          │
│                          ▼                                          │
│  dbt ──► staging views ──► 4 mart tables (MySQL)                   │
│                          │                                          │
│                          ▼                                          │
│  Airflow DAG ──► PostgreSQL analytics DB                            │
└─────────────────────────────────────────────────────────────────────┘
```

---

## ✅ Prerequisites

| Tool | Min Version | Check |
|---|---|---|
| Git | 2.30+ | `git --version` |
| Docker Engine | 24.0+ | `docker --version` |
| Docker Compose | 2.20+ | `docker compose version` |

**Resources:** 8 GB RAM minimum (16 GB recommended), 15 GB free disk.

---

## 🚀 Quick Start

```bash
# 1. Clone
git clone https://github.com/YOUR_ORG/payment-gateway-pipeline.git
cd payment-gateway-pipeline

# 2. Configure
cp .env.example .env
nano .env          # fill in MinIO, PostgreSQL, and Airflow passwords

# 3. Create Airflow directories
mkdir -p dags logs plugins

# 4. Build and start (all services start in correct order automatically)
docker compose pull
docker compose build
docker compose up -d

# 5. Watch seeding (3-8 minutes)
docker compose logs -f payment_data_seeder
# Wait for: ✅  DATA GENERATION COMPLETE

# 6. Verify Debezium is streaming
curl http://localhost:8083/connectors/payment-gateway-cdc/status
```

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---|---|---|
| Adminer (MySQL) | http://localhost:8081 | Server: `mysql` · User: `pg_user` · Pass: `pg_password_2024` |
| Kafdrop (Kafka) | http://localhost:9000 | None required |
| Debezium REST | http://localhost:8083/connectors | None required |
| MinIO Console | http://localhost:9001 | User: `minioadmin` · Pass: from `.env` |
| Airflow UI | http://localhost:8082 | User: `admin` · Pass: from `.env` |

---

## 📁 Project Structure

```
.
├── docker-compose.yml
├── .env.example
├── Dockerfile.airflow           # Custom Airflow + dbt image
│
├── init-scripts/
│   ├── 00_debezium_grants.sql   # MySQL privileges for Debezium (runs first)
│   └── init.sql                 # Schema + 13 tables + reference data
│
├── Data-Sources/
│   ├── Dockerfile
│   ├── entrypoint.py            # Pure Python MySQL readiness checker
│   ├── generate_data.py         # Bulk seeder (155k rows, 6 fraud patterns)
│   └── stream_data.py           # Live streamer (2 txns/sec, 1 fraud/hr)
│
├── Pipeline/
│   ├── Dockerfile.pipeline
│   ├── entrypoint_pipeline.py   # Pure Python service readiness entrypoint
│   ├── wait_for.py              # TCP + schema readiness checker
│   ├── kafka_consumer.py        # Debezium CDC → Parquet → MinIO
│   └── debezium-connector.json  # Debezium MySQL connector config
│
├── dbt/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/             # Views: stg_transactions, stg_clients, ...
│       └── marts/               # Tables: daily_summary, merchant, client_360, fraud
│
├── dags/
│   ├── dag_dbt_hourly_staging.py     # Hourly: refresh staging views
│   ├── dag_dbt_daily_marts.py        # Daily 02:00: full dbt run + tests
│   ├── dag_load_marts_to_postgres.py # Daily 03:00: MySQL → PostgreSQL
│   └── dag_pipeline_monitor.py       # Every 15 min: health checks
│
└── postgres-init/
    ├── 01_create_analytics_db.sql    # Create analytics database
    └── 02_analytics_schema.sql       # Create mart tables in PostgreSQL
```

---

## 🔄 CDC — Change Data Capture

This pipeline uses **Debezium** (not polling). Debezium reads MySQL's binary log directly:

| Feature | Polling (old) | Debezium CDC (current) |
|---|---|---|
| Captures INSERTs | ✅ | ✅ |
| Captures UPDATEs | ❌ | ✅ |
| Captures DELETEs | ❌ | ✅ |
| Latency | 15 seconds | < 1 second |
| Initial load | Manual script | Automatic snapshot |

Debezium topics follow the pattern `cdc.payment_gateway.{table}`:
```
cdc.payment_gateway.transactions
cdc.payment_gateway.clients
cdc.payment_gateway.fraud_alerts
... (13 topics total)
```

Each message (after `ExtractNewRecordState` unwrap):
```json
{
  "txn_id": 1042, "amount": 142.50, "currency": "USD",
  "__op": "c",
  "__source_ts_ms": 1741954000000,
  "__table": "transactions",
  "__deleted": "false"
}
```

---

## 🗄️ Database Schema

13 tables across 6 domains:

```
Reference: countries · transaction_types · transaction_statuses
           card_networks · merchant_categories

Merchant:  merchants ──── pos_terminals

Client:    clients ──── payment_cards

Core:      transactions ──── fraud_alerts
                         └── refunds

Settlement: settlement_batches
```

**Seeded volumes:** 200 merchants · 1,000 POS · 10,000 clients · 155,000+ transactions · ~31,000 fraud alerts · 73,000 settlement batches

---

## 🪣 MinIO / Parquet Output

Files written as **Snappy-compressed Parquet** with Hive partitioning:

```
s3://payment-gateway/raw/transactions/year=2025/month=03/day=14/
    20250314_153022_abc123.parquet   ← ~180 KB, 1,000 rows
```

Read with Pandas:
```python
import pandas as pd
df = pd.read_parquet(
    "s3://payment-gateway/raw/transactions/year=2025/month=03/day=14/",
    storage_options={"endpoint_url": "http://localhost:9002",
                     "key": "minioadmin", "secret": "minioadmin123"}
)
```

Or DuckDB:
```sql
SELECT channel, COUNT(*) AS txns, SUM(amount) AS revenue
FROM read_parquet('s3://payment-gateway/raw/transactions/**/*.parquet')
GROUP BY channel ORDER BY revenue DESC
```

---

## ⚙️ Airflow DAGs

| DAG | Schedule | Action |
|---|---|---|
| `dbt_hourly_staging` | Every hour :05 | Refresh staging views + tests |
| `dbt_daily_marts` | Daily 02:00 UTC | Full dbt run (staging → 4 marts in parallel → tests → docs) |
| `load_marts_to_postgres` | Daily 03:00 UTC | TRUNCATE + reload all mart tables into PostgreSQL analytics DB |
| `pipeline_monitor` | Every 15 min | MySQL health · ingestion rate · fraud spike · unreviewed alerts · dbt freshness |

---

## 🔧 Configuration

| Variable | Default | Description |
|---|---|---|
| `MINIO_ROOT_USER` | — | MinIO admin username (**required**) |
| `MINIO_ROOT_PASSWORD` | — | MinIO admin password (**required**) |
| `POSTGRES_USER` | — | Airflow DB username (**required**) |
| `POSTGRES_PASSWORD` | — | Airflow DB password (**required**) |
| `FLUSH_ROWS` | 1000 | Rows before Parquet flush |
| `FLUSH_SECONDS` | 30 | Seconds before Parquet flush |
| `PARQUET_COMPRESSION` | snappy | `snappy` / `gzip` / `zstd` / `none` |
| `SKIP_DELETES` | true | Skip CDC DELETE events |

---

## 🛠️ Common Commands

```bash
# Full reset — wipe ALL data and restart clean
docker compose down -v && docker compose up --build -d

# Check Debezium connector status
curl http://localhost:8083/connectors/payment-gateway-cdc/status

# Restart Debezium connector (e.g. after MySQL restart)
curl -X POST http://localhost:8083/connectors/payment-gateway-cdc/restart

# Check Kafka consumer lag
docker exec -it kafka kafka-consumer-groups \
  --bootstrap-server localhost:9092 \
  --describe --group pg-minio-consumer

# Query analytics DB directly
docker exec -it airflow-postgres \
  psql -U airflow -d analytics \
  -c "SELECT txn_date, txn_count, gross_amount FROM mart_daily_transaction_summary ORDER BY txn_date DESC LIMIT 5;"
```

---

## ❓ Troubleshooting

| Symptom | Fix |
|---|---|
| `RELOAD privilege` error in Debezium | `00_debezium_grants.sql` not run → `docker compose down -v && up --build` |
| `payment_gateway_db` unhealthy | Increase `start_period` in MySQL healthcheck; check `docker logs payment_gateway_db` |
| Debezium connector state = FAILED | `curl localhost:8083/connectors/payment-gateway-cdc/status` — check MySQL grants and binlog flags |
| No `cdc.*` topics in Kafdrop | Snapshot not started yet; check `docker logs kafka_connect` |
| No Parquet files in MinIO | Check `docker compose ps kafka_consumer` and `docker logs kafka_consumer` |
| `data_seeder` exits code 1 | `docker compose down -v && docker compose up --build` |

---

## 📄 Documentation

| File | Contents |
|---|---|
| `docs/pipeline_documentation.docx` | Full technical reference: schema, CDC, Debezium, Parquet, dbt, Airflow |
| `docs/deployment_guide.docx` | Step-by-step deployment with troubleshooting |

---

## 📦 Tech Stack

![MySQL](https://img.shields.io/badge/MySQL-8.0-4479A1?logo=mysql&logoColor=white)
![Debezium](https://img.shields.io/badge/Debezium-2.4-FF4500?logo=apache&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache_Kafka-3.4-231F20?logo=apachekafka&logoColor=white)
![MinIO](https://img.shields.io/badge/MinIO-S3_Compatible-C72E49?logo=minio&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)
![dbt](https://img.shields.io/badge/dbt-1.7-FF694B?logo=dbt&logoColor=white)
![Airflow](https://img.shields.io/badge/Airflow-2.9.3-017CEE?logo=apacheairflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-336791?logo=postgresql&logoColor=white)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)
![Parquet](https://img.shields.io/badge/Parquet-PyArrow-50ABF1)
