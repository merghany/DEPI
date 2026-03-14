#!/bin/bash
# ─────────────────────────────────────────────────────────────────────────────
# entrypoint_pipeline.sh
#
# Waits for every upstream service the pipeline needs before handing off.
# Checks requested via WAIT_FOR env var (comma-separated):
#   mysql   — waits for MySQL TCP + schema tables
#   kafka   — waits for Kafka broker to accept connections
#   minio   — waits for MinIO HTTP endpoint to respond
# ─────────────────────────────────────────────────────────────────────────────
set -e

MAX_RETRIES=90
INTERVAL=3

# Which services to wait for (set in docker-compose per service)
WAIT_FOR="${WAIT_FOR:-kafka}"

# ── Helper: wait for TCP port ─────────────────────────────────────────────────
wait_tcp() {
    local host=$1 port=$2 label=$3
    echo "⏳  [$label] Waiting for TCP $host:$port …"
    for i in $(seq 1 $MAX_RETRIES); do
        if nc -z "$host" "$port" 2>/dev/null; then
            echo "✅  [$label] TCP $host:$port is open (attempt $i)"
            return 0
        fi
        echo "   [$label] Attempt $i/$MAX_RETRIES — sleeping ${INTERVAL}s …"
        sleep $INTERVAL
    done
    echo "❌  [$label] Timed out waiting for $host:$port"
    exit 1
}

# ── Wait for MySQL (connection + schema) ─────────────────────────────────────
wait_mysql() {
    local host="${DB_HOST:-mysql}" port="${DB_PORT:-3306}"
    local user="${DB_USER:-pg_user}" pass="${DB_PASSWORD:-pg_password_2024}"
    local db="${DB_NAME:-payment_gateway}"

    wait_tcp "$host" "$port" "MySQL"

    echo "⏳  [MySQL] Waiting for schema (transaction_statuses table) …"
    for i in $(seq 1 $MAX_RETRIES); do
        if python3 - <<EOF 2>/dev/null
import mysql.connector, sys
try:
    c = mysql.connector.connect(host="$host", port=$port,
                                user="$user", password="$pass",
                                database="$db", connection_timeout=3)
    cur = c.cursor()
    cur.execute("""SELECT COUNT(*) FROM information_schema.tables
                   WHERE table_schema='$db'
                   AND table_name='transaction_statuses'""")
    ok = cur.fetchone()[0] > 0
    cur.close(); c.close()
    sys.exit(0 if ok else 1)
except: sys.exit(1)
EOF
        then
            echo "✅  [MySQL] Schema ready (attempt $i)"
            return 0
        fi
        echo "   [MySQL] Attempt $i/$MAX_RETRIES — sleeping ${INTERVAL}s …"
        sleep $INTERVAL
    done
    echo "❌  [MySQL] Schema not ready in time"
    exit 1
}

# ── Wait for Kafka (TCP + broker metadata reachable) ─────────────────────────
wait_kafka() {
    local host="${KAFKA_HOST:-kafka}" port="${KAFKA_PORT:-9092}"

    wait_tcp "$host" "$port" "Kafka"

    echo "⏳  [Kafka] Waiting for broker to accept Kafka connections …"
    for i in $(seq 1 $MAX_RETRIES); do
        if python3 - <<EOF 2>/dev/null
from kafka import KafkaConsumer
try:
    # KafkaConsumer.topics() is the correct API to probe broker readiness
    consumer = KafkaConsumer(
        bootstrap_servers="$host:$port",
        group_id=None,
        request_timeout_ms=5000,
        connections_max_idle_ms=8000,
    )
    consumer.topics()   # triggers metadata fetch from the broker
    consumer.close()
    exit(0)
except Exception:
    exit(1)
EOF
        then
            echo "✅  [Kafka] Broker ready (attempt $i)"
            return 0
        fi
        echo "   [Kafka] Attempt $i/$MAX_RETRIES — sleeping ${INTERVAL}s …"
        sleep $INTERVAL
    done
    echo "❌  [Kafka] Broker not ready in time"
    exit 1
}

# ── Wait for MinIO (HTTP /minio/health/live) ──────────────────────────────────
wait_minio() {
    local host="${MINIO_HOST:-minio}" port="${MINIO_API_PORT:-9000}"

    wait_tcp "$host" "$port" "MinIO"

    echo "⏳  [MinIO] Waiting for MinIO health endpoint …"
    for i in $(seq 1 $MAX_RETRIES); do
        if python3 - <<EOF 2>/dev/null
import urllib.request, sys
try:
    r = urllib.request.urlopen("http://$host:$port/minio/health/live", timeout=3)
    sys.exit(0 if r.status == 200 else 1)
except: sys.exit(1)
EOF
        then
            echo "✅  [MinIO] Health endpoint OK (attempt $i)"
            return 0
        fi
        echo "   [MinIO] Attempt $i/$MAX_RETRIES — sleeping ${INTERVAL}s …"
        sleep $INTERVAL
    done
    echo "❌  [MinIO] Not ready in time"
    exit 1
}

# ── Dispatch ──────────────────────────────────────────────────────────────────
IFS=',' read -ra SERVICES <<< "$WAIT_FOR"
for svc in "${SERVICES[@]}"; do
    case "$svc" in
        mysql) wait_mysql ;;
        kafka) wait_kafka ;;
        minio) wait_minio ;;
        *) echo "⚠️   Unknown service '$svc' — skipping" ;;
    esac
done

echo ""
echo "🚀  All services ready. Handing off to: $*"
echo ""
exec "$@"
