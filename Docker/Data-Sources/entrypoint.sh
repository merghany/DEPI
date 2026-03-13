#!/bin/bash
# ──────────────────────────────────────────────────────────────
# entrypoint.sh
# Two-phase wait:
#   Phase 1 — TCP connection accepted  (MySQL process is up)
#   Phase 2 — key schema tables exist  (init.sql has finished)
# ──────────────────────────────────────────────────────────────
set -e

HOST="${DB_HOST:-mysql}"
PORT="${DB_PORT:-3306}"
USER="${DB_USER:-pg_user}"
PASS="${DB_PASSWORD:-pg_password_2024}"
DB="${DB_NAME:-payment_gateway}"
MAX_RETRIES=90          # 90 × 3 s = 4.5 min ceiling
RETRY_INTERVAL=3

# ── Phase 1: wait for a successful connection ──────────────────
echo "⏳  [Phase 1] Waiting for MySQL connection at ${HOST}:${PORT} …"

for i in $(seq 1 $MAX_RETRIES); do
    if python3 - <<EOF 2>/dev/null
import mysql.connector, sys
try:
    c = mysql.connector.connect(
        host="$HOST", port=$PORT,
        user="$USER", password="$PASS",
        database="$DB", connection_timeout=3)
    c.close()
    sys.exit(0)
except Exception as e:
    sys.exit(1)
EOF
    then
        echo "✅  [Phase 1] Connection established (attempt ${i})"
        break
    fi

    echo "   [Phase 1] Attempt ${i}/${MAX_RETRIES} — retrying in ${RETRY_INTERVAL}s …"
    sleep $RETRY_INTERVAL

    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "❌  MySQL did not accept connections in time. Exiting."
        exit 1
    fi
done

# ── Phase 2: wait for init.sql to finish (tables must exist) ──
# We probe for 'transaction_statuses' — the last reference table
# created by init.sql and required by generate_data.py on startup.
echo "⏳  [Phase 2] Waiting for schema tables to be ready …"

for i in $(seq 1 $MAX_RETRIES); do
    if python3 - <<EOF 2>/dev/null
import mysql.connector, sys
try:
    c = mysql.connector.connect(
        host="$HOST", port=$PORT,
        user="$USER", password="$PASS",
        database="$DB", connection_timeout=3)
    cur = c.cursor()
    # Check the last table created in init.sql
    cur.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = '$DB'
        AND   table_name   = 'transaction_statuses'
    """)
    count = cur.fetchone()[0]
    cur.close(); c.close()
    sys.exit(0 if count > 0 else 1)
except Exception as e:
    sys.exit(1)
EOF
    then
        echo "✅  [Phase 2] Schema is ready (attempt ${i})"
        break
    fi

    echo "   [Phase 2] Attempt ${i}/${MAX_RETRIES} — schema not ready yet, retrying in ${RETRY_INTERVAL}s …"
    sleep $RETRY_INTERVAL

    if [ "$i" -eq "$MAX_RETRIES" ]; then
        echo "❌  Schema tables did not appear in time. Exiting."
        exit 1
    fi
done

echo "🚀  Handing off to: $*"
exec "$@"
