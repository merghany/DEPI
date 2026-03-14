#!/usr/bin/env python3
"""
MySQL → Kafka Producer
======================
Two-phase operation:

  Phase 1 — FULL LOAD
    Reads every row from each configured table and publishes to the
    corresponding Kafka topic.  Runs once on startup (skipped if the
    topic already contains messages, i.e. a previous run completed).

  Phase 2 — INCREMENTAL CDC POLL
    Polls each table every POLL_INTERVAL seconds for rows whose
    watermark column (created_at / initiated_at) is newer than the
    last seen value and pushes only the delta.

Topics created automatically (one per table):
    pg.transactions        pg.clients          pg.merchants
    pg.pos_terminals       pg.payment_cards    pg.fraud_alerts
    pg.refunds             pg.settlement_batches

Message format (JSON):
    {
      "_meta": { "table": "transactions", "op": "r", "ts_ms": 1712345678000 },
      "payload": { ...row columns... }
    }

Requirements:
    pip install mysql-connector-python kafka-python

Usage:
    python kafka_producer.py [options]
    All options can also be set via environment variables.
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
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# ─────────────────────────────────────────────────────────────────────────────
# CLI / ENV config
# ─────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="MySQL → Kafka Producer")
parser.add_argument("--kafka-brokers",   default=os.getenv("KAFKA_BROKERS",   "kafka:9092"))
parser.add_argument("--db-host",         default=os.getenv("DB_HOST",         "mysql"))
parser.add_argument("--db-port",         type=int, default=int(os.getenv("DB_PORT", "3306")))
parser.add_argument("--db-user",         default=os.getenv("DB_USER",         "pg_user"))
parser.add_argument("--db-password",     default=os.getenv("DB_PASSWORD",     "pg_password_2024"))
parser.add_argument("--db-name",         default=os.getenv("DB_NAME",         "payment_gateway"))
parser.add_argument("--topic-prefix",    default=os.getenv("TOPIC_PREFIX",    "pg"))
parser.add_argument("--poll-interval",   type=int, default=int(os.getenv("POLL_INTERVAL", "15")),
                    help="Seconds between incremental polls (default: 15)")
parser.add_argument("--batch-size",      type=int, default=int(os.getenv("BATCH_SIZE", "500")),
                    help="Rows per Kafka send batch (default: 500)")
args = parser.parse_args()

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
# Each entry:  (table_name, watermark_column, order_column, pk_column)
# ─────────────────────────────────────────────────────────────────────────────
TABLES = [
    ("countries",           "created_at",   "country_id",   "country_id"),
    ("transaction_types",   None,           "type_id",      "type_id"),
    ("transaction_statuses",None,           "status_id",    "status_id"),
    ("card_networks",       None,           "network_id",   "network_id"),
    ("merchant_categories", None,           "mcc",          "mcc"),
    ("merchants",           "created_at",   "merchant_id",  "merchant_id"),
    ("pos_terminals",       "created_at",   "pos_id",       "pos_id"),
    ("clients",             "created_at",   "client_id",    "client_id"),
    ("payment_cards",       "created_at",   "card_id",      "card_id"),
    ("transactions",        "created_at",   "txn_id",       "txn_id"),
    ("fraud_alerts",        "created_at",   "alert_id",     "alert_id"),
    ("refunds",             "created_at",   "refund_id",    "refund_id"),
    ("settlement_batches",  "created_at",   "batch_id",     "batch_id"),
]

# ─────────────────────────────────────────────────────────────────────────────
# JSON serialiser — handles Decimal, date, datetime
# ─────────────────────────────────────────────────────────────────────────────
def _json_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
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

def fetch_batch(cur, table: str, order_col: str,
                offset: int, limit: int) -> list[dict]:
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

def ensure_topics(admin: KafkaAdminClient, tables: list):
    existing = set(admin.list_topics())
    to_create = []
    for table, *_ in tables:
        name = topic_name(table)
        if name not in existing:
            to_create.append(NewTopic(
                name=name,
                num_partitions=3,
                replication_factor=1,
                topic_configs={"retention.ms": str(7 * 24 * 3600 * 1000)},  # 7 days
            ))
    if to_create:
        try:
            admin.create_topics(to_create, validate_only=False)
            log.info("Created %d topics: %s",
                     len(to_create), [t.name for t in to_create])
        except TopicAlreadyExistsError:
            pass

def topic_has_messages(admin: KafkaAdminClient, table: str) -> bool:
    """Return True if the topic has at least one committed message."""
    from kafka import KafkaConsumer
    name = topic_name(table)
    try:
        consumer = KafkaConsumer(
            bootstrap_servers=args.kafka_brokers,
            group_id=None,
            enable_auto_commit=False,
        )
        partitions = consumer.partitions_for_topic(name) or set()
        from kafka import TopicPartition
        tps = [TopicPartition(name, p) for p in partitions]
        if not tps:
            consumer.close()
            return False
        end_offsets = consumer.end_offsets(tps)
        consumer.close()
        return any(v > 0 for v in end_offsets.values())
    except Exception:
        return False

def publish_rows(producer: KafkaProducer, table: str,
                 rows: list[dict], op: str = "r") -> int:
    """Publish a list of row dicts to the topic. Returns count sent."""
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
def full_load(producer: KafkaProducer, admin: KafkaAdminClient):
    log.info("═══ PHASE 1 — FULL LOAD ═══")
    db  = get_db()
    cur = db.cursor()

    for table, watermark_col, order_col, pk_col in TABLES:
        topic = topic_name(table)

        if topic_has_messages(admin, table):
            log.info("  %-25s SKIP — topic already has data", table)
            continue

        total    = row_count(cur, table)
        sent     = 0
        offset   = 0
        log.info("  %-25s full load  (%d rows) …", table, total)

        while offset < total:
            rows = fetch_batch(cur, table, order_col, offset, args.batch_size)
            if not rows:
                break
            n = publish_rows(producer, table, rows, op="r")
            producer.flush()
            sent   += n
            offset += n

        log.info("  %-25s ✓ %d rows published → %s", table, sent, topic)

    cur.close()
    db.close()
    log.info("═══ FULL LOAD COMPLETE ═══\n")

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2 — Incremental CDC Poll
# ─────────────────────────────────────────────────────────────────────────────
def incremental_poll(producer: KafkaProducer):
    log.info("═══ PHASE 2 — INCREMENTAL POLL (every %ds) ═══", args.poll_interval)

    # Initialise watermarks: start from (now - 1 minute) to catch any rows
    # written just before we began.
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

        for table, watermark_col, order_col, pk_col in TABLES:
            if not watermark_col:
                continue   # static reference tables — skip incremental

            since = watermarks[table]
            rows  = fetch_incremental(cur, table, watermark_col, order_col, since)

            if rows:
                n = publish_rows(producer, table, rows, op="c")
                producer.flush()
                total_new += n
                # Advance watermark to the latest created_at seen
                latest = max(
                    r[watermark_col] for r in rows
                    if isinstance(r.get(watermark_col), datetime)
                )
                watermarks[table] = latest
                log.info("  %-25s +%d new rows  (watermark → %s)", table, n, latest)

        if total_new:
            log.info("  Poll cycle complete — %d new rows published", total_new)
        else:
            log.debug("  Poll cycle — no new rows")

        # Reconnect every ~1 hour to avoid stale connections
        elapsed = time.time() - cycle_start
        sleep_s = max(0, args.poll_interval - elapsed)
        time.sleep(sleep_s)

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    log.info("🔌  Connecting to Kafka broker: %s", args.kafka_brokers)

    admin = KafkaAdminClient(
        bootstrap_servers=args.kafka_brokers,
        client_id="pg-producer-admin",
        request_timeout_ms=15_000,
    )

    producer = KafkaProducer(
        bootstrap_servers=args.kafka_brokers,
        client_id="pg-producer",
        acks="all",                         # wait for all in-sync replicas
        retries=5,
        retry_backoff_ms=500,
        batch_size=65536,                   # 64 KB batch
        linger_ms=20,                       # slight delay to coalesce messages
        compression_type="gzip",
        max_request_size=10 * 1024 * 1024,  # 10 MB
    )

    log.info("🗂️   Ensuring Kafka topics exist …")
    ensure_topics(admin, TABLES)

    # Phase 1
    full_load(producer, admin)

    # Phase 2
    try:
        incremental_poll(producer)
    except KeyboardInterrupt:
        log.info("\n🛑  Producer stopped.")
    finally:
        producer.flush()
        producer.close()
        admin.close()

if __name__ == "__main__":
    main()
