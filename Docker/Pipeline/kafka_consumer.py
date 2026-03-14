#!/usr/bin/env python3
"""
Kafka → MinIO Consumer
=======================
Subscribes to all pg.* topics, batches messages, and writes them
to MinIO as gzip-compressed JSON-Lines files organised by table and
ingestion date:

  Bucket layout:
    payment-gateway/
      raw/
        transactions/
          year=2025/month=03/day=14/
            20250314_153022_abc123.jsonl.gz
        clients/
          year=2025/month=03/day=14/
            ...
        ...

Flushing strategy (per table buffer):
  A buffer is flushed to MinIO when EITHER condition is met:
    • Buffer contains ≥ FLUSH_ROWS rows
    • ≥ FLUSH_SECONDS seconds have elapsed since the last flush

At startup the consumer also creates the MinIO bucket if it does
not already exist.

Requirements:
    pip install kafka-python minio

Usage:
    python kafka_consumer.py [options]
    All options can also be set via environment variables.
"""

import argparse
import gzip
import io
import json
import logging
import os
import signal
import sys
import threading
import time
import uuid
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from minio import Minio
from minio.error import S3Error

# ─────────────────────────────────────────────────────────────────────────────
# CLI / ENV config
# ─────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Kafka → MinIO Consumer")
parser.add_argument("--kafka-brokers",  default=os.getenv("KAFKA_BROKERS",  "kafka:9092"))
parser.add_argument("--topic-prefix",   default=os.getenv("TOPIC_PREFIX",   "pg"))
parser.add_argument("--group-id",       default=os.getenv("KAFKA_GROUP_ID", "pg-minio-consumer"))
parser.add_argument("--minio-endpoint", default=os.getenv("MINIO_ENDPOINT", "minio:9000"))
parser.add_argument("--minio-user",     default=os.getenv("MINIO_ROOT_USER",     "minioadmin"))
parser.add_argument("--minio-password", default=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin"))
parser.add_argument("--bucket",         default=os.getenv("MINIO_BUCKET",   "payment-gateway"))
parser.add_argument("--flush-rows",     type=int, default=int(os.getenv("FLUSH_ROWS",    "1000")))
parser.add_argument("--flush-seconds",  type=int, default=int(os.getenv("FLUSH_SECONDS", "30")))
parser.add_argument("--poll-ms",        type=int, default=int(os.getenv("POLL_MS",       "500")),
                    help="Kafka poll timeout in milliseconds")
args = parser.parse_args()

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("consumer")

# ─────────────────────────────────────────────────────────────────────────────
# Global state
# ─────────────────────────────────────────────────────────────────────────────
RUNNING = threading.Event()
RUNNING.set()

# Per-table buffers and last-flush timestamps
lock          = threading.Lock()
buffers:      dict[str, list[str]] = defaultdict(list)   # table → [jsonl lines]
last_flush:   dict[str, float]     = defaultdict(float)  # table → epoch time
flush_counts: dict[str, int]       = defaultdict(int)    # table → total files written
row_counts:   dict[str, int]       = defaultdict(int)    # table → total rows written

# ─────────────────────────────────────────────────────────────────────────────
# MinIO helpers
# ─────────────────────────────────────────────────────────────────────────────
def get_minio() -> Minio:
    return Minio(
        args.minio_endpoint,
        access_key=args.minio_user,
        secret_key=args.minio_password,
        secure=False,
    )

def ensure_bucket(client: Minio):
    if not client.bucket_exists(args.bucket):
        client.make_bucket(args.bucket)
        log.info("✅  Created MinIO bucket: %s", args.bucket)
    else:
        log.info("✅  MinIO bucket already exists: %s", args.bucket)

def object_key(table: str) -> str:
    """Build a Hive-partitioned object key for the current datetime."""
    now  = datetime.utcnow()
    ts   = now.strftime("%Y%m%d_%H%M%S")
    uid  = uuid.uuid4().hex[:8]
    return (
        f"raw/{table}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{ts}_{uid}.jsonl.gz"
    )

def flush_table(client: Minio, table: str, lines: list[str]):
    """Compress and upload a batch of JSON-Lines rows to MinIO."""
    if not lines:
        return

    # Build gzip-compressed JSON-Lines payload in memory
    buf = io.BytesIO()
    with gzip.GzipFile(fileobj=buf, mode="wb") as gz:
        gz.write(b"\n".join(l.encode() for l in lines) + b"\n")
    buf.seek(0)
    size = buf.getbuffer().nbytes
    key  = object_key(table)

    client.put_object(
        bucket_name=args.bucket,
        object_name=key,
        data=buf,
        length=size,
        content_type="application/gzip",
    )

    with lock:
        flush_counts[table] += 1
        row_counts[table]   += len(lines)

    log.info(
        "  %-25s → s3://%s/%s  (%d rows, %.1f KB)",
        table, args.bucket, key, len(lines), size / 1024,
    )

# ─────────────────────────────────────────────────────────────────────────────
# Buffer management
# ─────────────────────────────────────────────────────────────────────────────
def should_flush(table: str) -> bool:
    with lock:
        row_limit  = len(buffers[table]) >= args.flush_rows
        time_limit = (time.time() - last_flush[table]) >= args.flush_seconds
    return row_limit or (time_limit and buffers[table])

def do_flush(client: Minio, table: str):
    with lock:
        lines = buffers[table][:]
        buffers[table].clear()
        last_flush[table] = time.time()

    if lines:
        flush_table(client, table, lines)

def flush_all(client: Minio, force: bool = False):
    """Flush all tables that meet the flush criteria (or all if force=True)."""
    with lock:
        tables = list(buffers.keys())
    for table in tables:
        if force or should_flush(table):
            do_flush(client, table)

# ─────────────────────────────────────────────────────────────────────────────
# Periodic flush timer — runs in background thread
# ─────────────────────────────────────────────────────────────────────────────
def flush_timer(client: Minio):
    """Background thread: flush any table that has waited too long."""
    while RUNNING.is_set():
        time.sleep(5)
        flush_all(client)

# ─────────────────────────────────────────────────────────────────────────────
# Stats printer — runs in background thread
# ─────────────────────────────────────────────────────────────────────────────
START_TIME = time.time()

def stats_printer():
    while RUNNING.is_set():
        time.sleep(30)
        elapsed = int(time.time() - START_TIME)
        with lock:
            total_rows  = sum(row_counts.values())
            total_files = sum(flush_counts.values())
        log.info(
            "📊  Stats │ uptime %ds │ %d rows written │ %d files uploaded │ "
            "tables: %s",
            elapsed, total_rows, total_files,
            ", ".join(f"{t}={row_counts[t]}" for t in sorted(row_counts)),
        )

# ─────────────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────────────────────────────────────
minio_client_global: Minio = None

def shutdown(sig, frame):
    log.info("\n🛑  Shutdown signal received — flushing remaining buffers …")
    RUNNING.clear()
    if minio_client_global:
        flush_all(minio_client_global, force=True)
    elapsed = time.time() - START_TIME
    log.info("─" * 60)
    log.info("  Total runtime  : %ds", int(elapsed))
    log.info("  Total rows     : %d", sum(row_counts.values()))
    log.info("  Total files    : %d", sum(flush_counts.values()))
    for table in sorted(row_counts):
        log.info("  %-25s rows=%-8d files=%d",
                 table, row_counts[table], flush_counts[table])
    log.info("─" * 60)
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ─────────────────────────────────────────────────────────────────────────────
# Topic discovery — find all pg.* topics
# ─────────────────────────────────────────────────────────────────────────────
def discover_topics(consumer: KafkaConsumer) -> list[str]:
    all_topics = consumer.topics()
    matched    = [t for t in all_topics if t.startswith(f"{args.topic_prefix}.")]
    log.info("🔍  Discovered %d topics: %s", len(matched), matched)
    return matched

# ─────────────────────────────────────────────────────────────────────────────
# Main consumer loop
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global minio_client_global

    # ── MinIO setup ──────────────────────────────────────────────────────────
    log.info("🪣  Connecting to MinIO at %s …", args.minio_endpoint)
    minio_client = get_minio()
    minio_client_global = minio_client
    ensure_bucket(minio_client)

    # ── Kafka consumer ────────────────────────────────────────────────────────
    log.info("📡  Connecting to Kafka broker: %s …", args.kafka_brokers)
    consumer = KafkaConsumer(
        bootstrap_servers=args.kafka_brokers,
        group_id=args.group_id,
        auto_offset_reset="earliest",       # start from the very first message
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        max_poll_records=500,
        fetch_max_bytes=52428800,           # 50 MB
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )

    # Discover and subscribe to all pg.* topics
    topics = discover_topics(consumer)
    if not topics:
        log.warning("⚠️   No topics found matching prefix '%s.' — "
                    "waiting 10 s then retrying …", args.topic_prefix)
        time.sleep(10)
        topics = discover_topics(consumer)

    if not topics:
        log.error("❌  Still no topics found. Is the producer running?")
        sys.exit(1)

    consumer.subscribe(topics)
    log.info("✅  Subscribed to: %s", topics)

    # Initialise flush timestamps
    with lock:
        for t in topics:
            table = t.split(".", 1)[1]
            last_flush[table] = time.time()

    # Background threads
    threading.Thread(target=flush_timer,   args=(minio_client,), daemon=True, name="flush-timer").start()
    threading.Thread(target=stats_printer, daemon=True,           name="stats").start()

    log.info(
        "🚀  Consumer running │ flush_rows=%d │ flush_seconds=%ds │ bucket=%s",
        args.flush_rows, args.flush_seconds, args.bucket,
    )

    # ── Main poll loop ────────────────────────────────────────────────────────
    msg_count = 0
    try:
        while RUNNING.is_set():
            records = consumer.poll(timeout_ms=args.poll_ms)

            for tp, messages in records.items():
                raw_topic = tp.topic                            # e.g. pg.transactions
                table     = raw_topic.split(".", 1)[1]         # e.g. transactions

                for msg in messages:
                    try:
                        payload = msg.value.get("payload", msg.value)
                        line    = json.dumps(payload, default=str)
                        with lock:
                            buffers[table].append(line)
                        msg_count += 1
                    except Exception as e:
                        log.warning("Failed to deserialise message on %s: %s", raw_topic, e)

            # Flush tables that have hit the row threshold immediately
            with lock:
                hot_tables = [t for t, lines in buffers.items()
                              if len(lines) >= args.flush_rows]
            for table in hot_tables:
                do_flush(minio_client, table)

    except Exception as e:
        log.error("Consumer error: %s", e, exc_info=True)
    finally:
        consumer.close()
        flush_all(minio_client, force=True)
        log.info("Consumer shut down after processing %d messages.", msg_count)

if __name__ == "__main__":
    main()
