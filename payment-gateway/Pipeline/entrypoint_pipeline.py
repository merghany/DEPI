#!/usr/bin/env python3
"""
entrypoint_pipeline.py
======================
Single-file Docker ENTRYPOINT — no shell required.

1. Reads WAIT_FOR env var (comma-separated: mysql, kafka, minio)
2. Waits for every listed service to be ready using only Python stdlib
3. os.execvp() into the CMD passed by docker-compose

Usage (set in Dockerfile):
    ENTRYPOINT ["python3", "/app/entrypoint_pipeline.py"]
    CMD ["python3", "kafka_producer.py"]
"""

import os
import sys
import time
import socket
import urllib.request

# ── Config ────────────────────────────────────────────────────────────────────
MAX_RETRIES = 90
INTERVAL    = 3

def log(msg: str):
    print(msg, flush=True)

# ── TCP check ─────────────────────────────────────────────────────────────────
def tcp_open(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False

def wait_tcp(host: str, port: int, label: str):
    log(f"⏳  [{label}] Waiting for TCP {host}:{port} …")
    for i in range(1, MAX_RETRIES + 1):
        if tcp_open(host, port):
            log(f"✅  [{label}] {host}:{port} is open (attempt {i})")
            return
        log(f"   [{label}] Attempt {i}/{MAX_RETRIES} — retrying in {INTERVAL}s …")
        time.sleep(INTERVAL)
    log(f"❌  [{label}] Timed out after {MAX_RETRIES * INTERVAL}s")
    sys.exit(1)

# ── Service waiters ───────────────────────────────────────────────────────────
def wait_kafka():
    host = os.environ.get("KAFKA_HOST", "kafka")
    port = int(os.environ.get("KAFKA_PORT", "9092"))
    wait_tcp(host, port, "Kafka")
    log(f"✅  [Kafka] Broker at {host}:{port} is accepting connections")


def wait_mysql():
    host = os.environ.get("DB_HOST",     "mysql")
    port = int(os.environ.get("DB_PORT", "3306"))
    user = os.environ.get("DB_USER",     "pg_user")
    pw   = os.environ.get("DB_PASSWORD", "pg_password_2024")
    db   = os.environ.get("DB_NAME",     "payment_gateway")

    wait_tcp(host, port, "MySQL")

    log("⏳  [MySQL] Waiting for schema (transaction_statuses) …")
    import mysql.connector
    for i in range(1, MAX_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=host, port=port, user=user,
                password=pw, database=db,
                connection_timeout=3,
            )
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema = %s AND table_name = 'transaction_statuses'",
                (db,),
            )
            ready = cur.fetchone()[0] > 0
            cur.close()
            conn.close()
            if ready:
                log(f"✅  [MySQL] Schema ready (attempt {i})")
                return
        except Exception:
            pass
        log(f"   [MySQL] Attempt {i}/{MAX_RETRIES} — retrying in {INTERVAL}s …")
        time.sleep(INTERVAL)
    log("❌  [MySQL] Schema not ready after max retries")
    sys.exit(1)


def wait_minio():
    host = os.environ.get("MINIO_HOST",     "minio")
    port = int(os.environ.get("MINIO_API_PORT", "9000"))

    wait_tcp(host, port, "MinIO")

    url = f"http://{host}:{port}/minio/health/live"
    log(f"⏳  [MinIO] Waiting for health endpoint {url} …")
    for i in range(1, MAX_RETRIES + 1):
        try:
            with urllib.request.urlopen(url, timeout=3) as r:
                if r.status == 200:
                    log(f"✅  [MinIO] Healthy (attempt {i})")
                    return
        except Exception:
            pass
        log(f"   [MinIO] Attempt {i}/{MAX_RETRIES} — retrying in {INTERVAL}s …")
        time.sleep(INTERVAL)
    log("❌  [MinIO] Health endpoint not ready after max retries")
    sys.exit(1)


# ── Dispatch ──────────────────────────────────────────────────────────────────
HANDLERS = {
    "kafka": wait_kafka,
    "mysql": wait_mysql,
    "minio": wait_minio,
}

def main():
    wait_for = os.environ.get("WAIT_FOR", "kafka")
    services = [s.strip() for s in wait_for.split(",") if s.strip()]

    log(f"🔍  Services to wait for: {services}")

    for svc in services:
        if svc in HANDLERS:
            HANDLERS[svc]()
        else:
            log(f"⚠️   Unknown service '{svc}' — skipping")

    log("\n🚀  All services ready.\n")

    # Hand off to CMD — os.execvp replaces this process entirely.
    # sys.argv[1:] contains the CMD from docker-compose.
    cmd = sys.argv[1:]
    if not cmd:
        log("❌  No command provided. Set CMD in Dockerfile or docker-compose.")
        sys.exit(1)

    os.execvp(cmd[0], cmd)


if __name__ == "__main__":
    main()
