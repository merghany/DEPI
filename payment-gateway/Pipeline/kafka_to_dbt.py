#!/usr/bin/env python3
"""
kafka_to_dbt.py  —  Kafka CDC → dbt
======================================
Subscribes to all Debezium CDC topics (cdc.payment_gateway.*),
counts incoming events per table, and triggers dbt runs
directly against MySQL when thresholds are met.

Trigger strategy (two conditions, either fires a run):
  • Row threshold  — accumulated CDC events  >= TRIGGER_ROWS  (default 500)
  • Time threshold — seconds since last run  >= TRIGGER_SECONDS (default 60)

dbt run order:
  1. dbt run --select staging   (refresh all staging views)
  2. dbt run --select marts     (rebuild all mart tables)
  3. dbt test                   (data quality checks — optional)

All connection params and dbt paths come from environment variables.

Requirements:
    pip install kafka-python-ng mysql-connector-python dbt-core dbt-mysql
"""

import logging
import os
import subprocess
import sys
import threading
import time
import json
from collections import defaultdict
from datetime import datetime

from kafka import KafkaConsumer

# ── Config ────────────────────────────────────────────────────────────────────
KAFKA_BROKERS   = os.environ.get("KAFKA_BROKERS",    "kafka:9092")
TOPIC_PREFIX    = os.environ.get("TOPIC_PREFIX",     "cdc")
DB_NAME         = os.environ.get("DB_NAME",          "payment_gateway")
KAFKA_GROUP_ID  = os.environ.get("KAFKA_GROUP_ID",   "pg-dbt-trigger")
TRIGGER_ROWS    = int(os.environ.get("TRIGGER_ROWS",    "500"))
TRIGGER_SECONDS = int(os.environ.get("TRIGGER_SECONDS", "60"))
RUN_TESTS       = os.environ.get("RUN_TESTS", "false").lower() == "true"
POLL_MS         = int(os.environ.get("POLL_MS", "500"))

DBT_PROJECT_DIR  = os.environ.get("DBT_PROJECT_DIR",  "/app/dbt")
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR", "/app/dbt")

CDC_TOPIC_PREFIX = f"{TOPIC_PREFIX}.{DB_NAME}"

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("kafka-to-dbt")

# ── State ─────────────────────────────────────────────────────────────────────
lock            = threading.Lock()
pending_rows    = defaultdict(int)   # table → unsent CDC event count
last_dbt_run    = 0.0                # epoch of last dbt run
total_events    = 0
total_dbt_runs  = 0
START_TIME      = time.time()
DBT_RUNNING     = threading.Event()  # prevents concurrent dbt runs

def log_state():
    with lock:
        pending = dict(pending_rows)
        elapsed = int(time.time() - last_dbt_run)
    log.info(
        "📊  uptime=%ds | total_events=%d | dbt_runs=%d | "
        "pending=%s | last_run=%ds ago",
        int(time.time() - START_TIME),
        total_events, total_dbt_runs, pending, elapsed,
    )

# ── dbt runner ────────────────────────────────────────────────────────────────
def run_dbt(reason: str):
    """Run dbt staging + marts in a background thread."""
    global total_dbt_runs, last_dbt_run

    if DBT_RUNNING.is_set():
        log.info("⏭️   dbt already running — skipping trigger (%s)", reason)
        return

    def _run():
        global total_dbt_runs, last_dbt_run
        DBT_RUNNING.set()
        started = time.time()
        log.info("🔧  [dbt] Starting run — reason: %s", reason)

        # dbt-mysql 1.7 treats --profiles-dir and --project-dir as
        # subcommand-level flags (after "run"), not global flags (before "run").
        # Pass them after the subcommand and also set env vars as fallback.
        dbt_env = {**os.environ,
                   "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
                   "DBT_PROJECT_DIR":  DBT_PROJECT_DIR}

        def dbt_cmd(subcmd: list) -> list:
            return [
                "dbt" #,
        #        "--profiles-dir", DBT_PROFILES_DIR,
         #       "--project-dir",  DBT_PROJECT_DIR,
            ] + subcmd
        
        steps = [
            (dbt_cmd(["run", "--select", "staging"]), "staging"),
            (dbt_cmd(["run", "--select", "marts"]),   "marts"),
        ]
        if RUN_TESTS:
            steps.append((dbt_cmd(["test"]), "test"))

        success = True
        for cmd, label in steps:
            cmd = dbt_cmd(subcmd)    
            log.info("   [dbt] Running: %s", " ".join(cmd))
            result = subprocess.run(
                        cmd,
                        env=dbt_env,
                        cwd=DBT_PROJECT_DIR,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.STDOUT,
                        text=True
                    )
            if result.returncode == 0:
                log.info("   [dbt] ✅  %s complete", label)
            else:
                log.error(f"   [dbt] ❌  {reason} failed:\n{result.stdout}")
                success = False
                break

        elapsed = time.time() - started
        with lock:
            last_dbt_run = time.time()
            total_dbt_runs += 1
            pending_rows.clear()

        status = "✅  SUCCESS" if success else "❌  FAILED"
        log.info("🔧  [dbt] Run complete — %s  (%.1fs)", status, elapsed)
        DBT_RUNNING.clear()

    threading.Thread(target=_run, daemon=True, name="dbt-runner").start()


def should_trigger() -> tuple[bool, str]:
    """Return (trigger, reason) based on current state."""
    with lock:
        total_pending = sum(pending_rows.values())
        elapsed       = time.time() - last_dbt_run

    if total_pending >= TRIGGER_ROWS:
        return True, f"{total_pending} pending CDC events >= threshold {TRIGGER_ROWS}"
    if elapsed >= TRIGGER_SECONDS and total_pending > 0:
        return True, f"{int(elapsed)}s elapsed >= {TRIGGER_SECONDS}s and {total_pending} pending events"
    return False, ""


# ── Timer thread — periodic trigger check ────────────────────────────────────
def timer_thread():
    while True:
        time.sleep(10)
        trigger, reason = should_trigger()
        if trigger:
            run_dbt(reason)
        log_state()

# ── Stats printer ─────────────────────────────────────────────────────────────
def stats_thread():
    while True:
        time.sleep(60)
        log_state()

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    global total_events, last_dbt_run
    last_dbt_run = time.time()

    log.info("📡  Connecting to Kafka at %s …", KAFKA_BROKERS)
    log.info("🎯  CDC prefix  : %s.*", CDC_TOPIC_PREFIX)
    log.info("⚙️   Trigger rows: %d  |  Trigger seconds: %d", TRIGGER_ROWS, TRIGGER_SECONDS)
    log.info("📂  dbt project : %s", DBT_PROJECT_DIR)
    log.info("🧪  Run tests   : %s", RUN_TESTS)

    import re
    consumer = KafkaConsumer(
        bootstrap_servers       = KAFKA_BROKERS,
        group_id                = KAFKA_GROUP_ID,
        auto_offset_reset       = "earliest",
        enable_auto_commit      = True,
        auto_commit_interval_ms = 5000,
        max_poll_records        = 500,
        value_deserializer      = lambda v: json.loads(v.decode()) if v else None,
    )

    pattern = f"^{re.escape(CDC_TOPIC_PREFIX)}\\..+"
    consumer.subscribe(pattern=pattern)
    log.info("✅  Subscribed to pattern: %s", pattern)
    log.info("    Waiting for CDC events from Debezium snapshot …")

    # Start background threads
    threading.Thread(target=timer_thread, daemon=True, name="timer").start()
    threading.Thread(target=stats_thread, daemon=True, name="stats").start()

    try:
        while True:
            records = consumer.poll(timeout_ms=POLL_MS)

            for tp, messages in records.items():
                # cdc.payment_gateway.transactions → transactions
                table = tp.topic.split(".")[-1]

                for msg in messages:
                    if msg.value is None:
                        continue   # tombstone

                    op      = msg.value.get("__op", "r")
                    deleted = msg.value.get("__deleted", "false")

                    # Count every non-tombstone event
                    with lock:
                        pending_rows[table] += 1
                    total_events += 1

                    if total_events % 10000 == 0:
                        log.info("   Consumed %d CDC events total …", total_events)

            # Check trigger after each poll batch
            trigger, reason = should_trigger()
            if trigger:
                run_dbt(reason)

    except KeyboardInterrupt:
        log.info("🛑  Shutting down …")
        # Run one final dbt pass to capture any pending events
        with lock:
            remaining = sum(pending_rows.values())
        if remaining > 0:
            log.info("   Running final dbt pass (%d pending events) …", remaining)
            run_dbt("shutdown flush")
            time.sleep(10)
    finally:
        consumer.close()
        log.info("Done. Total events: %d  |  dbt runs: %d", total_events, total_dbt_runs)

if __name__ == "__main__":
    main()
