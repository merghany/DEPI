#!/usr/bin/env python3
"""
entrypoint.py — MySQL readiness checker for Data-Sources container
==================================================================
Two-phase wait:
  Phase 1 — TCP connection accepted  (MySQL process is up)
  Phase 2 — transaction_statuses table exists (init.sql has finished)

After both phases pass, os.execvp() into the CMD supplied by docker-compose.

All connection params read from environment variables:
  DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME
"""

import os
import sys
import time
import socket

MAX_RETRIES    = 90   # 90 × 3 s = 4.5 min ceiling
RETRY_INTERVAL = 3

def log(msg: str):
    print(msg, flush=True)

# ── Phase 1 — TCP connection ──────────────────────────────────────────────────
def wait_tcp(host: str, port: int):
    log(f"⏳  [Phase 1] Waiting for MySQL TCP at {host}:{port} …")
    for i in range(1, MAX_RETRIES + 1):
        try:
            with socket.create_connection((host, port), timeout=3):
                log(f"✅  [Phase 1] Connection established (attempt {i})")
                return
        except OSError:
            pass
        log(f"   [Phase 1] Attempt {i}/{MAX_RETRIES} — retrying in {RETRY_INTERVAL}s …")
        time.sleep(RETRY_INTERVAL)
    log("❌  MySQL did not accept connections in time. Exiting.")
    sys.exit(1)

# ── Phase 2 — Schema ready ────────────────────────────────────────────────────
def wait_schema(host: str, port: int, user: str, password: str, db: str):
    log("⏳  [Phase 2] Waiting for schema tables (transaction_statuses) …")
    import mysql.connector
    for i in range(1, MAX_RETRIES + 1):
        try:
            conn = mysql.connector.connect(
                host=host, port=port, user=user,
                password=password, database=db,
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
                log(f"✅  [Phase 2] Schema is ready (attempt {i})")
                return
        except Exception:
            pass
        log(f"   [Phase 2] Attempt {i}/{MAX_RETRIES} — schema not ready, retrying in {RETRY_INTERVAL}s …")
        time.sleep(RETRY_INTERVAL)
    log("❌  Schema tables did not appear in time. Exiting.")
    sys.exit(1)

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    host     = os.environ.get("DB_HOST",     "mysql")
    port     = int(os.environ.get("DB_PORT", "3306"))
    user     = os.environ.get("DB_USER",     "pg_user")
    password = os.environ.get("DB_PASSWORD", "pg_password_2024")
    db       = os.environ.get("DB_NAME",     "payment_gateway")

    wait_tcp(host, port)
    wait_schema(host, port, user, password, db)

    cmd = sys.argv[1:]
    if not cmd:
        log("❌  No command provided. Set CMD in Dockerfile or docker-compose.")
        sys.exit(1)

    log(f"🚀  Handing off to: {' '.join(cmd)}")
    os.execvp(cmd[0], cmd)

if __name__ == "__main__":
    main()
