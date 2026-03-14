#!/usr/bin/env python3
"""
wait_for.py — Service readiness checker
Usage: python3 wait_for.py mysql kafka minio
Reads connection params from environment variables.
Exits 0 only when ALL requested services are ready.
"""
import os, sys, time, socket, urllib.request

MAX_RETRIES = 90
INTERVAL    = 3

def tcp_open(host, port):
    s = socket.socket()
    s.settimeout(3)
    try:
        s.connect((host, int(port)))
        s.close()
        return True
    except Exception:
        return False

def wait_kafka():
    host = os.getenv("KAFKA_HOST", "kafka")
    port = os.getenv("KAFKA_PORT", "9092")
    print(f"⏳  [Kafka] Waiting for TCP {host}:{port} …", flush=True)
    for i in range(1, MAX_RETRIES + 1):
        if tcp_open(host, port):
            print(f"✅  [Kafka] {host}:{port} open (attempt {i})", flush=True)
            return
        print(f"   [Kafka] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …", flush=True)
        time.sleep(INTERVAL)
    print("❌  [Kafka] Timed out", flush=True)
    sys.exit(1)

def wait_mysql():
    import mysql.connector
    host = os.getenv("DB_HOST", "mysql")
    port = int(os.getenv("DB_PORT", "3306"))
    user = os.getenv("DB_USER", "pg_user")
    pw   = os.getenv("DB_PASSWORD", "pg_password_2024")
    db   = os.getenv("DB_NAME", "payment_gateway")

    print(f"⏳  [MySQL] Waiting for TCP {host}:{port} …", flush=True)
    for i in range(1, MAX_RETRIES + 1):
        if tcp_open(host, str(port)):
            print(f"✅  [MySQL] TCP open (attempt {i})", flush=True)
            break
        print(f"   [MySQL] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …", flush=True)
        time.sleep(INTERVAL)
    else:
        print("❌  [MySQL] TCP timed out", flush=True)
        sys.exit(1)

    print("⏳  [MySQL] Waiting for schema …", flush=True)
    for i in range(1, MAX_RETRIES + 1):
        try:
            c = mysql.connector.connect(
                host=host, port=port, user=user,
                password=pw, database=db, connection_timeout=3)
            cur = c.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM information_schema.tables "
                "WHERE table_schema=%s AND table_name='transaction_statuses'", (db,))
            ok = cur.fetchone()[0] > 0
            cur.close(); c.close()
            if ok:
                print(f"✅  [MySQL] Schema ready (attempt {i})", flush=True)
                return
        except Exception:
            pass
        print(f"   [MySQL] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …", flush=True)
        time.sleep(INTERVAL)
    print("❌  [MySQL] Schema timed out", flush=True)
    sys.exit(1)

def wait_minio():
    host = os.getenv("MINIO_HOST", "minio")
    port = os.getenv("MINIO_API_PORT", "9000")

    print(f"⏳  [MinIO] Waiting for TCP {host}:{port} …", flush=True)
    for i in range(1, MAX_RETRIES + 1):
        if tcp_open(host, port):
            print(f"✅  [MinIO] TCP open (attempt {i})", flush=True)
            break
        print(f"   [MinIO] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …", flush=True)
        time.sleep(INTERVAL)
    else:
        print("❌  [MinIO] TCP timed out", flush=True)
        sys.exit(1)

    print("⏳  [MinIO] Waiting for health endpoint …", flush=True)
    for i in range(1, MAX_RETRIES + 1):
        try:
            r = urllib.request.urlopen(
                f"http://{host}:{port}/minio/health/live", timeout=3)
            if r.status == 200:
                print(f"✅  [MinIO] Healthy (attempt {i})", flush=True)
                return
        except Exception:
            pass
        print(f"   [MinIO] Attempt {i}/{MAX_RETRIES} — sleeping {INTERVAL}s …", flush=True)
        time.sleep(INTERVAL)
    print("❌  [MinIO] Health timed out", flush=True)
    sys.exit(1)

HANDLERS = {"mysql": wait_mysql, "kafka": wait_kafka, "minio": wait_minio}

if __name__ == "__main__":
    services = sys.argv[1:]
    if not services:
        print("Usage: wait_for.py mysql kafka minio", flush=True)
        sys.exit(1)
    for svc in services:
        svc = svc.strip()
        if svc in HANDLERS:
            HANDLERS[svc]()
        else:
            print(f"⚠️   Unknown service '{svc}' — skipping", flush=True)
    print("\n🚀  All services ready.\n", flush=True)
