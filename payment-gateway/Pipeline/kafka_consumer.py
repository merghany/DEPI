#!/usr/bin/env python3
"""
Kafka CDC Consumer  (Debezium → MinIO Parquet)
===============================================
Subscribes to all Debezium CDC topics ( cdc.payment_gateway.* ),
buffers messages per table, and writes columnar Parquet files to
MinIO using Hive-style date partitioning.

Debezium topic naming:
    cdc.payment_gateway.transactions
    cdc.payment_gateway.clients
    ...

After ExtractNewRecordState unwrap each message is a flat row dict
with extra fields added by Debezium:
    __op           r=snapshot  c=insert  u=update  d=delete
    __source_ts_ms binlog event timestamp (milliseconds)
    __table        source table name
    __deleted      "true" for DELETE events

Tombstone messages (null value) are skipped.
DELETE events are skipped by default (configurable).

Flush strategy:
    Flush to Parquet when EITHER condition is met:
      • Buffer >= FLUSH_ROWS rows
      • >= FLUSH_SECONDS elapsed since last flush

Requirements:
    pip install kafka-python-ng minio pyarrow
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
# CLI / ENV
# ─────────────────────────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Debezium CDC → MinIO Parquet Consumer")
parser.add_argument("--kafka-brokers",  default=os.getenv("KAFKA_BROKERS",      "kafka:9092"))
parser.add_argument("--topic-prefix",   default=os.getenv("TOPIC_PREFIX",       "cdc"))
parser.add_argument("--database",       default=os.getenv("DB_NAME",            "payment_gateway"))
parser.add_argument("--group-id",       default=os.getenv("KAFKA_GROUP_ID",     "pg-minio-consumer"))
parser.add_argument("--minio-endpoint", default=os.getenv("MINIO_ENDPOINT",     "minio:9000"))
parser.add_argument("--minio-user",     default=os.getenv("MINIO_ROOT_USER",    "minioadmin"))
parser.add_argument("--minio-password", default=os.getenv("MINIO_ROOT_PASSWORD","minioadmin"))
parser.add_argument("--bucket",         default=os.getenv("MINIO_BUCKET",       "payment-gateway"))
parser.add_argument("--flush-rows",     type=int, default=int(os.getenv("FLUSH_ROWS",    "100")))
parser.add_argument("--flush-seconds",  type=int, default=int(os.getenv("FLUSH_SECONDS", "10")))
parser.add_argument("--poll-ms",        type=int, default=int(os.getenv("POLL_MS",       "500")))
parser.add_argument("--compression",    default=os.getenv("PARQUET_COMPRESSION", "snappy"),
                    choices=["snappy","gzip","zstd","none"])
parser.add_argument("--skip-deletes",   action="store_true",
                    default=os.getenv("SKIP_DELETES","true").lower()=="true",
                    help="Skip DELETE events (default: true)")
args = parser.parse_args()

# Full Debezium topic prefix:  cdc.payment_gateway
CDC_TOPIC_PREFIX = f"{args.topic_prefix}.{args.database}"

# ─────────────────────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("cdc-consumer")

# ─────────────────────────────────────────────────────────────────────────────
# Global state
# ─────────────────────────────────────────────────────────────────────────────
RUNNING       = threading.Event()
RUNNING.set()
lock          = threading.Lock()
buffers:      dict[str, list[dict]] = defaultdict(list)
last_flush:   dict[str, float]      = defaultdict(float)
flush_counts: dict[str, int]        = defaultdict(int)
row_counts:   dict[str, int]        = defaultdict(int)
op_counts:    dict[str, dict]       = defaultdict(lambda: {"r":0,"c":0,"u":0,"d":0})
START_TIME    = time.time()

# ─────────────────────────────────────────────────────────────────────────────
# Type normalisation
# ─────────────────────────────────────────────────────────────────────────────
def normalise(value):
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
    rows = [normalise_row(r) for r in rows]
    if not rows:
        return b""
    all_keys = list(rows[0].keys())
    columns  = {k: [r.get(k) for r in rows] for k in all_keys}
    table    = pa.Table.from_pydict(columns)
    buf      = io.BytesIO()
    pq.write_table(
        table, buf,
        compression     = args.compression if args.compression != "none" else None,
        write_statistics= True,
        use_dictionary  = True,
        row_group_size  = min(len(rows), 100_000),
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
        log.info("✅  Created bucket: %s", args.bucket)
    else:
        log.info("✅  Bucket exists: %s", args.bucket)

def object_key(table: str) -> str:
    now = datetime.utcnow()
    return (
        f"raw/{table}/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}.parquet"
    )

def flush_table(client: Minio, table: str, rows: list[dict]):
    if not rows:
        return
    data = rows_to_parquet(rows)
    if not data:
        return
    key = object_key(table)
    client.put_object(
        bucket_name  = args.bucket,
        object_name  = key,
        data         = io.BytesIO(data),
        length       = len(data),
        content_type = "application/octet-stream",
    )
    with lock:
        flush_counts[table] += 1
        row_counts[table]   += len(rows)
    log.info("  %-30s → s3://%s/%s  (%d rows, %.1f KB, %s)",
             table, args.bucket, key, len(rows), len(data)/1024, args.compression)

# ─────────────────────────────────────────────────────────────────────────────
# Buffer management
# ─────────────────────────────────────────────────────────────────────────────
def should_flush(table: str) -> bool:
    with lock:
        return (len(buffers[table]) >= args.flush_rows or
                (time.time() - last_flush[table] >= args.flush_seconds
                 and bool(buffers[table])))

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
    for t in tables:
        if force or should_flush(t):
            do_flush(client, t)

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
            ops = {t: dict(v) for t, v in op_counts.items()}
        log.info("📊  uptime=%ds | rows=%d | files=%d | ops=%s",
                 elapsed, total_rows, total_files, ops)

# ─────────────────────────────────────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────────────────────────────────────
_minio_global: Minio = None

def shutdown(sig, frame):
    log.info("🛑  Shutdown — flushing buffers …")
    RUNNING.clear()
    if _minio_global:
        flush_all(_minio_global, force=True)
    elapsed = time.time() - START_TIME
    log.info("─" * 60)
    log.info("  Runtime : %ds", int(elapsed))
    log.info("  Rows    : %d", sum(row_counts.values()))
    log.info("  Files   : %d", sum(flush_counts.values()))
    for t in sorted(row_counts):
        log.info("  %-30s rows=%-8d files=%d ops=%s",
                 t, row_counts[t], flush_counts[t], dict(op_counts[t]))
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ─────────────────────────────────────────────────────────────────────────────
# Topic → table name
# ─────────────────────────────────────────────────────────────────────────────
def topic_to_table(topic: str) -> str:
    """
    cdc.payment_gateway.transactions  →  transactions
    """
    parts = topic.split(".")
    return parts[-1]  # always the table name

# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────
def main():
    global _minio_global

    log.info("🪣  Connecting to MinIO at %s …", args.minio_endpoint)
    minio_client  = get_minio()
    _minio_global = minio_client
    ensure_bucket(minio_client)

    log.info("📡  Connecting to Kafka at %s …", args.kafka_brokers)
    consumer = KafkaConsumer(
        bootstrap_servers       = args.kafka_brokers,
        group_id                = args.group_id,
        auto_offset_reset       = "earliest",
        enable_auto_commit      = True,
        auto_commit_interval_ms = 5000,
        max_poll_records        = 500,
        fetch_max_bytes         = 52428800,
        value_deserializer      = lambda v: json.loads(v.decode("utf-8")) if v else None,
    )

    # Pattern-based subscription — automatically picks up ALL cdc.payment_gateway.*
    # topics as Debezium creates them one by one during snapshot.
    # Static subscribe(list) would miss any topic created after the call.
    import re
    pattern = f"^{re.escape(CDC_TOPIC_PREFIX)}\\..+"
    consumer.subscribe(pattern=pattern)
    log.info("✅  Subscribed to pattern: %s", pattern)
    log.info("    Topics will be discovered automatically as Debezium creates them.")
    log.info("    Waiting for first messages (snapshot may take 5-15 minutes) ...")

    threading.Thread(target=flush_timer,   args=(minio_client,), daemon=True, name="flush").start()
    threading.Thread(target=stats_printer, daemon=True,           name="stats").start()

    log.info(
        "🚀  CDC Consumer running | flush_rows=%d | flush_seconds=%ds | "
        "compression=%s | skip_deletes=%s | bucket=%s",
        args.flush_rows, args.flush_seconds,
        args.compression, args.skip_deletes, args.bucket,
    )

    msg_count = 0
    skip_count = 0

    try:
        while RUNNING.is_set():
            records = consumer.poll(timeout_ms=args.poll_ms)

            for tp, messages in records.items():
                table = topic_to_table(tp.topic)

                for msg in messages:
                    raw = msg.value

                    # ── Tombstone (null value) — skip ─────────────────────────
                    if raw is None:
                        skip_count += 1
                        log.debug("Tombstone on %s — skip", tp.topic)
                        continue

                    # ── Extract CDC operation ─────────────────────────────────
                    # After ExtractNewRecordState unwrap:
                    #   __op           = r | c | u | d
                    #   __deleted      = "true" for deletes
                    #   __source_ts_ms = binlog timestamp
                    op      = raw.get("__op", "r")
                    deleted = raw.get("__deleted", "false")

                    # ── DELETE events ─────────────────────────────────────────
                    if deleted == "true" or op == "d":
                        if args.skip_deletes:
                            skip_count += 1
                            log.debug("DELETE on %s — skip", table)
                            continue
                        # If not skipping deletes, mark the row
                        raw["_deleted"] = True

                    # ── Strip Debezium internal fields, keep as metadata ───────
                    payload = {k: v for k, v in raw.items()
                               if not k.startswith("__")}

                    # Add clean CDC metadata columns
                    payload["_cdc_op"]     = op          # r/c/u/d
                    payload["_cdc_ts_ms"]  = raw.get("__source_ts_ms")
                    payload["_cdc_ingest"] = datetime.utcnow().isoformat()

                    with lock:
                        buffers[table].append(payload)
                        op_counts[table][op] += 1

                    msg_count += 1

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
        log.info("Shut down after %d messages (%d skipped).", msg_count, skip_count)

if __name__ == "__main__":
    main()
