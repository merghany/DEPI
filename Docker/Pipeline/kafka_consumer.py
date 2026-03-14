#!/usr/bin/env python3
"""
Kafka → MinIO Consumer  (Parquet format)
=========================================
Subscribes to all pg.* topics, buffers messages per table,
and writes columnar Parquet files to MinIO organised by
ingestion date:

  Bucket layout:
    payment-gateway/
      raw/
        transactions/
          year=2025/month=03/day=14/
            20250314_153022_abc123.parquet
        clients/
          year=2025/month=03/day=14/
            ...

Flushing strategy (per table buffer):
  • Buffer contains >= FLUSH_ROWS rows   — flush immediately
  • >= FLUSH_SECONDS elapsed since last flush — flush on timer

Requirements:
    pip install kafka-python-ng minio pyarrow

All options settable via environment variables.
"""

import argparse
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
from decimal import Decimal

import pyarrow as pa
import pyarrow.parquet as pq
from kafka import KafkaConsumer
from minio import Minio

# ─────────────────────────────────────────────────────────────────────────────
# CLI / ENV config
# ─────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Kafka → MinIO Parquet Consumer")
parser.add_argument("--kafka-brokers",  default=os.getenv("KAFKA_BROKERS",      "kafka:9092"))
parser.add_argument("--topic-prefix",   default=os.getenv("TOPIC_PREFIX",       "pg"))
parser.add_argument("--group-id",       default=os.getenv("KAFKA_GROUP_ID",     "pg-minio-consumer"))
parser.add_argument("--minio-endpoint", default=os.getenv("MINIO_ENDPOINT",     "minio:9000"))
parser.add_argument("--minio-user",     default=os.getenv("MINIO_ROOT_USER",    "minioadmin"))
parser.add_argument("--minio-password", default=os.getenv("MINIO_ROOT_PASSWORD","minioadmin"))
parser.add_argument("--bucket",         default=os.getenv("MINIO_BUCKET",       "payment-gateway"))
parser.add_argument("--flush-rows",     type=int, default=int(os.getenv("FLUSH_ROWS",    "1000")))
parser.add_argument("--flush-seconds",  type=int, default=int(os.getenv("FLUSH_SECONDS", "30")))
parser.add_argument("--poll-ms",        type=int, default=int(os.getenv("POLL_MS",       "500")))
parser.add_argument("--compression",    default=os.getenv("PARQUET_COMPRESSION", "snappy"),
                    choices=["snappy", "gzip", "zstd", "none"],
                    help="Parquet column compression codec (default: snappy)")
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
# Global state  (buffers store plain dicts, not JSON strings)
# ─────────────────────────────────────────────────────────────────────────────
RUNNING = threading.Event()
RUNNING.set()

lock:         threading.Lock            = threading.Lock()
buffers:      dict[str, list[dict]]     = defaultdict(list)   # table → [row dicts]
last_flush:   dict[str, float]          = defaultdict(float)  # table → epoch time
flush_counts: dict[str, int]            = defaultdict(int)    # files written per table
row_counts:   dict[str, int]            = defaultdict(int)    # rows written per table
START_TIME = time.time()

# ─────────────────────────────────────────────────────────────────────────────
# Type normalisation — make Python types safe for PyArrow inference
# ─────────────────────────────────────────────────────────────────────────────
def normalise(value):
    """Convert types that PyArrow cannot infer cleanly."""
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (dict, list)):
        return json.dumps(value, default=str)
    return value

def normalise_row(row: dict) -> dict:
    return {k: normalise(v) for k, v in row.items()}

# ─────────────────────────────────────────────────────────────────────────────
# Parquet writer
# ─────────────────────────────────────────────────────────────────────────────
def rows_to_parquet(rows: list[dict]) -> bytes:
    """
    Convert a list of row dicts to an in-memory Parquet file.
    Uses PyArrow's schema inference — no manual schema definition needed.
    """
    # Normalise types first
    rows = [normalise_row(r) for r in rows]

    # Build column-oriented dict from row-oriented list
    if not rows:
        return b""

    all_keys = list(rows[0].keys())
    columns  = {k: [r.get(k) for r in rows] for k in all_keys}

    table = pa.Table.from_pydict(columns)
    buf   = io.BytesIO()
    pq.write_table(
        table, buf,
        compression=args.compression if args.compression != "none" else None,
        # Embed pandas metadata so the file is directly readable by pandas/spark
        write_statistics=True,
        use_dictionary=True,       # dictionary encoding for low-cardinality columns
        row_group_size=min(len(rows), 100_000),
    )
    return buf.getvalue()

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
    """Hive-partitioned Parquet object key."""
    now = datetime.utcnow()
    ts  = now.strftime("%Y%m%d_%H%M%S")
    uid = uuid.uuid4().hex[:8]
    return (
        f"raw/{table}/"
        f"year={now.year}/"
        f"month={now.month:02d}/"
        f"day={now.day:02d}/"
        f"{ts}_{uid}.parquet"
    )

def flush_table(client: Minio, table: str, rows: list[dict]):
    """Serialise rows to Parquet and upload to MinIO."""
    if not rows:
        return

    data = rows_to_parquet(rows)
    if not data:
        return

    size = len(data)
    key  = object_key(table)

    client.put_object(
        bucket_name  = args.bucket,
        object_name  = key,
        data         = io.BytesIO(data),
        length       = size,
        content_type = "application/octet-stream",
    )

    with lock:
        flush_counts[table] += 1
        row_counts[table]   += len(rows)

    log.info(
        "  %-25s → s3://%s/%s  (%d rows, %.1f KB, %s)",
        table, args.bucket, key, len(rows), size / 1024, args.compression,
    )

# ─────────────────────────────────────────────────────────────────────────────
# Buffer management
# ─────────────────────────────────────────────────────────────────────────────
def should_flush(table: str) -> bool:
    with lock:
        row_limit  = len(buffers[table]) >= args.flush_rows
        time_limit = (time.time() - last_flush[table]) >= args.flush_seconds
    return row_limit or (time_limit and bool(buffers[table]))

def do_flush(client: Minio, table: str):
    with lock:
        rows = buffers[table][:]
        buffers[table].clear()
        last_flush[table] = time.time()
    if rows:
        flush_table(client, table, rows)

def flush_all(client: Minio, force: bool = False):
    with lock:
        tables = list(buffers.keys())
    for table in tables:
        if force or should_flush(table):
            do_flush(client, table)

# ─────────────────────────────────────────────────────────────────────────────
# Background threads
# ─────────────────────────────────────────────────────────────────────────────
def flush_timer(client: Minio):
    while RUNNING.is_set():
        time.sleep(5)
        flush_all(client)

def stats_printer():
    while RUNNING.is_set():
        time.sleep(30)
        elapsed = int(time.time() - START_TIME)
        with lock:
            total_rows  = sum(row_counts.values())
            total_files = sum(flush_counts.values())
        log.info(
            "📊  uptime=%ds | rows=%d | files=%d | tables: %s",
            elapsed, total_rows, total_files,
            ", ".join(f"{t}={row_counts[t]}" for t in sorted(row_counts)),
        )

# ─────────────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────────────────────────────────────
_minio_global: Minio = None

def shutdown(sig, frame):
    log.info("🛑  Shutdown received — flushing remaining buffers …")
    RUNNING.clear()
    if _minio_global:
        flush_all(_minio_global, force=True)
    elapsed = time.time() - START_TIME
    log.info("─" * 60)
    log.info("  Runtime : %ds", int(elapsed))
    log.info("  Rows    : %d", sum(row_counts.values()))
    log.info("  Files   : %d", sum(flush_counts.values()))
    for t in sorted(row_counts):
        log.info("  %-25s rows=%-8d files=%d", t, row_counts[t], flush_counts[t])
    log.info("─" * 60)
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global _minio_global

    log.info("🪣  Connecting to MinIO at %s …", args.minio_endpoint)
    minio_client = get_minio()
    _minio_global = minio_client
    ensure_bucket(minio_client)

    log.info("📡  Connecting to Kafka at %s …", args.kafka_brokers)
    consumer = KafkaConsumer(
        bootstrap_servers      = args.kafka_brokers,
        group_id               = args.group_id,
        auto_offset_reset      = "earliest",
        enable_auto_commit     = True,
        auto_commit_interval_ms= 5000,
        max_poll_records       = 500,
        fetch_max_bytes        = 52428800,   # 50 MB
        value_deserializer     = lambda v: json.loads(v.decode("utf-8")),
    )

    # Discover pg.* topics with retry
    for attempt in range(1, 7):
        topics = [t for t in consumer.topics()
                  if t.startswith(f"{args.topic_prefix}.")]
        if topics:
            break
        log.warning("⏳  No topics found (attempt %d/6) — retrying in 10s …", attempt)
        time.sleep(10)

    if not topics:
        log.error("❌  No pg.* topics found. Is the producer running?")
        sys.exit(1)

    consumer.subscribe(topics)
    log.info("✅  Subscribed to %d topics: %s", len(topics), topics)

    with lock:
        for t in topics:
            last_flush[t.split(".", 1)[1]] = time.time()

    threading.Thread(target=flush_timer,   args=(minio_client,), daemon=True, name="flush").start()
    threading.Thread(target=stats_printer, daemon=True,           name="stats").start()

    log.info(
        "🚀  Running | flush_rows=%d | flush_seconds=%ds | compression=%s | bucket=%s",
        args.flush_rows, args.flush_seconds, args.compression, args.bucket,
    )

    msg_count = 0
    try:
        while RUNNING.is_set():
            records = consumer.poll(timeout_ms=args.poll_ms)

            for tp, messages in records.items():
                table = tp.topic.split(".", 1)[1]   # pg.transactions → transactions

                for msg in messages:
                    try:
                        payload = msg.value.get("payload", msg.value)
                        with lock:
                            buffers[table].append(payload)
                        msg_count += 1
                    except Exception as e:
                        log.warning("Deserialise error on %s: %s", tp.topic, e)

            # Immediate flush for hot tables
            with lock:
                hot = [t for t, rows in buffers.items()
                       if len(rows) >= args.flush_rows]
            for table in hot:
                do_flush(minio_client, table)

    except Exception as e:
        log.error("Consumer error: %s", e, exc_info=True)
    finally:
        consumer.close()
        flush_all(minio_client, force=True)
        log.info("Shut down after %d messages.", msg_count)

if __name__ == "__main__":
    main()
