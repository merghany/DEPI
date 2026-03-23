#!/usr/bin/env python3
"""
MySQL → Kafka Producer
======================
Two-phase operation:

  Phase 1 — FULL LOAD
    Reads every row from each configured table and publishes to the
    corresponding Kafka topic.

    Idempotency: a compacted state topic (pg.__producer_state) records
    completion per table. On restart, only tables without a completed
    marker are re-loaded. Wiping Kafka volumes clears the markers and
    triggers a fresh full load.

  Phase 2 — INCREMENTAL CDC POLL
    Polls each table every POLL_INTERVAL seconds for rows whose
    watermark column (created_at) is newer than the last seen value.

Message format (JSON):
    {
      "_meta": { "table": "transactions", "op": "r", "ts_ms": 1712345678000 },
      "payload": { ...row columns... }
    }

Requirements:
    pip install mysql-connector-python kafka-python-ng
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, date, timedelta
from decimal import Decimal

import mysql.connector
from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError
from kafka import TopicPartition

# ─────────────────────────────────────────────────────────────────────────────
# CLI / ENV
# ─────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="MySQL → Kafka Producer")
parser.add_argument("--kafka-brokers",  default=os.getenv("KAFKA_BROKERS",  "kafka:9092"))
parser.add_argument("--db-host",        default=os.getenv("DB_HOST",        "mysql"))
parser.add_argument("--db-port",        type=int, default=int(os.getenv("DB_PORT", "3306")))
parser.add_argument("--db-user",        default=os.getenv("DB_USER",        "pg_user"))
parser.add_argument("--db-password",    default=os.getenv("DB_PASSWORD",    "pg_password_2024"))
parser.add_argument("--db-name",        default=os.getenv("DB_NAME",        "payment_gateway"))
parser.add_argument("--topic-prefix",   default=os.getenv("TOPIC_PREFIX",   "pg"))
parser.add_argument("--poll-interval",  type=int, default=int(os.getenv("POLL_INTERVAL", "15")))
parser.add_argument("--batch-size",     type=int, default=int(os.getenv("BATCH_SIZE",    "500")))
args = parser.parse_args()

STATE_TOPIC = f"{args.topic_prefix}.__producer_state"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("producer")

# ─────────────────────────────────────────────────────────────────────────────
# Table catalogue
# ─────────────────────────────────────────────────────────────────────────────
TABLES = [
    ("countries",           "created_at",  "country_id",  "country_id"),
    ("transaction_types",   None,          "type_id",     "type_id"),
    ("transaction_statuses",None,          "status_id",   "status_id"),
    ("card_networks",       None,          "network_id",  "network_id"),
    ("merchant_categories", None,          "mcc",         "mcc"),
    ("merchants",           "created_at",  "merchant_id", "merchant_id"),
    ("pos_terminals",       "created_at",  "pos_id",      "pos_id"),
    ("clients",             "created_at",  "client_id",   "client_id"),
    ("payment_cards",       "created_at",  "card_id",     "card_id"),
    ("transactions",        "created_at",  "txn_id",      "txn_id"),
    ("fraud_alerts",        "created_at",  "alert_id",    "alert_id"),
    ("refunds",             "created_at",  "refund_id",   "refund_id"),
    ("settlement_batches",  "created_at",  "batch_id",    "batch_id"),
]

# ─────────────────────────────────────────────────────────────────────────────
# JSON serialiser
# ─────────────────────────────────────────────────────────────────────────────
def _json_default(obj):
    if isinstance(obj, Decimal):       return float(obj)
    if isinstance(obj, (datetime, date)): return obj.isoformat()
    raise TypeError(f"Cannot serialise {type(obj)}")

def to_json(payload: dict) -> bytes:
    return json.dumps(payload, default=_json_default).encode("utf-8")

# ─────────────────────────────────────────────────────────────────────────────
# MySQL helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_db():
    return mysql.connector.connect(
        host=args.db_host, port=args.db_port,
        user=args.db_user, password=args.db_password,
        database=args.db_name, autocommit=True,
    )

def row_count(cur, table: str) -> int:
    cur.execute(f"SELECT COUNT(*) FROM `{table}`")
    return cur.fetchone()[0]

def fetch_batch(cur, table: str, order_col: str, offset: int, limit: int) -> list[dict]:
    cur.execute(
        f"SELECT * FROM `{table}` ORDER BY `{order_col}` LIMIT %s OFFSET %s",
        (limit, offset)
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def fetch_incremental(cur, table: str, watermark_col: str,
                      order_col: str, since: datetime) -> list[dict]:
    cur.execute(
        f"SELECT * FROM `{table}` WHERE `{watermark_col}` > %s ORDER BY `{order_col}`",
        (since,)
    )
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

# ─────────────────────────────────────────────────────────────────────────────
# Kafka helpers
# ─────────────────────────────────────────────────────────────────────────────
def topic_name(table: str) -> str:
    return f"{args.topic_prefix}.{table}"

def ensure_topics(admin: KafkaAdminClient):
    """Create all data topics + the compacted state topic."""
    existing = set(KafkaConsumer(
        bootstrap_servers=args.kafka_brokers,
        group_id=None,
        request_timeout_ms=10_000,
    ).topics())

    to_create = []

    # Data topics
    for table, *_ in TABLES:
        name = topic_name(table)
        if name not in existing:
            to_create.append(NewTopic(
                name=name,
                num_partitions=3,
                replication_factor=1,
                topic_configs={"retention.ms": str(7 * 24 * 3600 * 1000)},
            ))

    # State topic — compacted so only the latest value per key is kept.
    # This means after a full load completes the marker survives restarts
    # but is erased when Kafka storage is wiped (docker compose down -v).
    if STATE_TOPIC not in existing:
        to_create.append(NewTopic(
            name=STATE_TOPIC,
            num_partitions=1,
            replication_factor=1,
            topic_configs={
                "cleanup.policy": "compact",
                "retention.ms":   "-1",         # keep forever (until compacted away)
                "min.cleanable.dirty.ratio": "0.01",
            },
        ))

    if to_create:
        try:
            admin.create_topics(to_create, validate_only=False)
            log.info("Created topics: %s", [t.name for t in to_create])
        except TopicAlreadyExistsError:
            pass

# ─────────────────────────────────────────────────────────────────────────────
# State topic — read / write completion markers
# ─────────────────────────────────────────────────────────────────────────────
def read_completed_tables() -> set[str]:
    """
    Read the compacted state topic and return the set of table names
    whose full load has already been marked as complete.
    Returns an empty set if the topic is empty (first run or after wipe).
    """
    consumer = KafkaConsumer(
        STATE_TOPIC,
        bootstrap_servers=args.kafka_brokers,
        group_id=None,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda v: json.loads(v.decode()),
        consumer_timeout_ms=5_000,   # stop after 5 s of no new messages
    )

    completed = set()
    try:
        for msg in consumer:
            key   = msg.key.decode() if msg.key else None
            value = msg.value
            if key and value and value.get("status") == "completed":
                completed.add(key)
            elif key and value and value.get("status") == "reset":
                completed.discard(key)
    except Exception:
        pass
    finally:
        consumer.close()

    if completed:
        log.info("State topic: %d tables already fully loaded: %s",
                 len(completed), sorted(completed))
    else:
        log.info("State topic: no completed tables found — full load required for all tables")

    return completed


def mark_table_completed(producer: KafkaProducer, table: str, rows_sent: int):
    """Write a completion marker for table into the state topic."""
    payload = {
        "status":     "completed",
        "rows":       rows_sent,
        "completed_at": datetime.utcnow().isoformat(),
    }
    producer.send(
        STATE_TOPIC,
        key=table.encode(),
        value=to_json(payload),
    )
    producer.flush()
    log.info("  %-25s state → completed (%d rows)", table, rows_sent)

# ─────────────────────────────────────────────────────────────────────────────
# Publish rows
# ─────────────────────────────────────────────────────────────────────────────
def publish_rows(producer: KafkaProducer, table: str,
                 rows: list[dict], op: str = "r") -> int:
    ts_ms = int(time.time() * 1000)
    topic = topic_name(table)
    for row in rows:
        msg = {"_meta": {"table": table, "op": op, "ts_ms": ts_ms},
               "payload": row}
        producer.send(topic, value=to_json(msg))
    return len(rows)

# ─────────────────────────────────────────────────────────────────────────────
# Phase 1 — Full Load
# ─────────────────────────────────────────────────────────────────────────────
def full_load(producer: KafkaProducer):
    log.info("═══ PHASE 1 — FULL LOAD ═══")

    # Read which tables were already fully loaded in a previous run
    completed = read_completed_tables()

    db  = get_db()
    cur = db.cursor()

    for table, watermark_col, order_col, pk_col in TABLES:
        if table in completed:
            log.info("  %-25s SKIP — already loaded (state topic marker found)", table)
            continue

        total  = row_count(cur, table)
        sent   = 0
        offset = 0
        log.info("  %-25s full load (%d rows) …", table, total)

        while offset < total:
            rows = fetch_batch(cur, table, order_col, offset, args.batch_size)
            if not rows:
                break
            n = publish_rows(producer, table, rows, op="r")
            producer.flush()
            sent   += n
            offset += n

        log.info("  %-25s ✓ %d rows → %s", table, sent, topic_name(table))

        # Write completion marker so restarts skip this table
        mark_table_completed(producer, table, sent)

    cur.close()
    db.close()
    log.info("═══ FULL LOAD COMPLETE ═══\n")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Incremental CDC Poll
# ─────────────────────────────────────────────────────────────────────────────
def incremental_poll(producer: KafkaProducer):
    log.info("═══ PHASE 2 — INCREMENTAL POLL (every %ds) ═══", args.poll_interval)

    watermarks: dict[str, datetime] = {}
    start = datetime.now() - timedelta(minutes=1)
    for table, watermark_col, *_ in TABLES:
        if watermark_col:
            watermarks[table] = start

    db  = get_db()
    cur = db.cursor()

    while True:
        cycle_start = time.time()
        total_new   = 0

        for table, watermark_col, order_col, _ in TABLES:
            if not watermark_col:
                continue

            since = watermarks[table]
            rows  = fetch_incremental(cur, table, watermark_col, order_col, since)

            if rows:
                n = publish_rows(producer, table, rows, op="c")
                producer.flush()
                total_new += n
                latest = max(
                    r[watermark_col] for r in rows
                    if isinstance(r.get(watermark_col), datetime)
                )
                watermarks[table] = latest
                log.info("  %-25s +%d new rows (watermark → %s)", table, n, latest)

        if total_new:
            log.info("  Poll cycle — %d new rows published", total_new)
        else:
            log.debug("  Poll cycle — no new rows")

        time.sleep(max(0, args.poll_interval - (time.time() - cycle_start)))

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("🔌  Connecting to Kafka: %s", args.kafka_brokers)

    admin = KafkaAdminClient(
        bootstrap_servers=args.kafka_brokers,
        client_id="pg-producer-admin",
        request_timeout_ms=15_000,
    )
    producer = KafkaProducer(
        bootstrap_servers=args.kafka_brokers,
        client_id="pg-producer",
        acks="all",
        retries=5,
        retry_backoff_ms=500,
        batch_size=65536,
        linger_ms=20,
        compression_type="gzip",
        max_request_size=10 * 1024 * 1024,
    )

    log.info("🗂️   Ensuring topics exist …")
    ensure_topics(admin)

    full_load(producer)

    try:
        incremental_poll(producer)
    except KeyboardInterrupt:
        log.info("🛑  Producer stopped.")
    finally:
        producer.flush()
        producer.close()
        admin.close()

if __name__ == "__main__":
    main()
