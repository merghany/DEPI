#!/usr/bin/env python3
"""
wait_for.py — pure Python service readiness checker.
Usage: python3 wait_for.py <service> [<service> ...]
Services: mysql | kafka | minio
All connection params come from environment variables.
"""
import os, sys, time, socket, urllib.request

MAX_RETRIES = 90
INTERVAL    = 3

def log(msg): print(msg, flush=True)

def tcp_open(host, port, timeout=3):
    try:
        s = socket.create_connection((host, int(port)), timeout=timeout)
        s.close()
        return True
    except Exception:
        return False

def wait_tcp(host, port, label):
    log(f"⏳  [{label}] Waiting for TCP {host}:{port} …")
    for i in range(1, MAX_RETRIES + 1):
        if tcp_open(host, port):
            log(f"✅  [{label}] {host}:{port} open (attempt {i})")
            return
        log(f"   [{label}] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …")
        time.sleep(INTERVAL)
    log(f"❌  [{label}] Timed out after {MAX_RETRIES} attempts")
    sys.exit(1)

# ── mysql ─────────────────────────────────────────────────────────
def wait_mysql():
    host = os.environ.get("DB_HOST",     "mysql")
    port = os.environ.get("DB_PORT",     "3306")
    user = os.environ.get("DB_USER",     "pg_user")
    pw   = os.environ.get("DB_PASSWORD", "pg_password_2024")
    db   = os.environ.get("DB_NAME",     "payment_gateway")

    wait_tcp(host, port, "MySQL")

    log("⏳  [MySQL] Waiting for schema …")
    import mysql.connector
    for i in range(1, MAX_RETRIES + 1):
        try:
            c = mysql.connector.connect(
                host=host, port=int(port), user=user,
                password=pw, database=db, connection_timeout=3)
            cur = c.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name='transaction_statuses'",
                (db,))
            ready = cur.fetchone()[0] > 0
            cur.close(); c.close()
            if ready:
                log(f"✅  [MySQL] Schema ready (attempt {i})")
                return
        except Exception:
            pass
        log(f"   [MySQL] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …")
        time.sleep(INTERVAL)
    log("❌  [MySQL] Schema timed out")
    sys.exit(1)

# ── kafka ─────────────────────────────────────────────────────────
def wait_kafka():
    host = os.environ.get("KAFKA_HOST", "localhost")
    port = os.environ.get("KAFKA_PORT", "9092")
    wait_tcp(host, port, "Kafka")

# ── minio ─────────────────────────────────────────────────────────
def wait_minio():
    host = os.environ.get("MINIO_HOST",     "minio")
    port = os.environ.get("MINIO_API_PORT", "9000")

    wait_tcp(host, port, "MinIO")

    log("⏳  [MinIO] Waiting for health endpoint …")
    url = f"http://{host}:{port}/minio/health/live"
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = urllib.request.urlopen(url, timeout=3)
            if r.status == 200:
                log(f"✅  [MinIO] Healthy (attempt {i})")
                return
        except Exception:
            pass
        log(f"   [MinIO] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …")
        time.sleep(INTERVAL)
    log("❌  [MinIO] Health timed out")
    sys.exit(1)

HANDLERS = {"mysql": wait_mysql, "kafka": wait_kafka, "minio": wait_minio}

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: wait_for.py mysql kafka minio")
        sys.exit(1)
    for svc in sys.argv[1:]:
        svc = svc.strip()
        if svc in HANDLERS:
            HANDLERS[svc]()
        else:
            log(f"⚠️  Unknown service '{svc}' — skipping")
    log("\n🚀  All services ready.\n")
