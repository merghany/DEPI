#!/usr/bin/env python3
"""
register_connector.py
=====================
Registers the Debezium MySQL CDC connector with Kafka Connect.
Deletes any existing connector first to ensure a clean registration.
Retries until Kafka Connect is ready — no shell, no quoting issues.
"""

import json
import sys
import time
import urllib.request
import urllib.error

CONNECT_URL    = "http://kafka-connect:8083"
CONNECTOR_NAME = "payment-gateway-cdc"
CONNECTOR_FILE = "/connector.json"
MAX_WAIT       = 120   # seconds to wait for Kafka Connect to be ready
RETRY_INTERVAL = 5

def log(msg):
    print(msg, flush=True)

def get(path):
    with urllib.request.urlopen(f"{CONNECT_URL}{path}", timeout=5) as r:
        return r.status, json.loads(r.read())

def delete(path):
    req = urllib.request.Request(
        f"{CONNECT_URL}{path}", method="DELETE"
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code

def post(path, data: dict):
    body = json.dumps(data).encode()
    req  = urllib.request.Request(
        f"{CONNECT_URL}{path}",
        data    = body,
        method  = "POST",
        headers = {"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())

# ── Step 1: Wait for Kafka Connect REST API ──────────────────────────────────
log("⏳  Waiting for Kafka Connect REST API ...")
for i in range(1, MAX_WAIT // RETRY_INTERVAL + 1):
    try:
        status, _ = get("/connectors")
        if status == 200:
            log(f"✅  Kafka Connect is ready (attempt {i})")
            break
    except Exception:
        pass
    log(f"   Not ready yet — retrying in {RETRY_INTERVAL}s (attempt {i}) ...")
    time.sleep(RETRY_INTERVAL)
else:
    log("❌  Kafka Connect did not become ready in time.")
    sys.exit(1)

# ── Step 2: Delete existing connector (if any) ───────────────────────────────
log(f"🗑️   Removing existing connector '{CONNECTOR_NAME}' (if any) ...")
code = delete(f"/connectors/{CONNECTOR_NAME}")
if code == 204:
    log("   Deleted successfully. Waiting 3s for cleanup ...")
    time.sleep(3)
elif code == 404:
    log("   No existing connector found — nothing to delete.")
else:
    log(f"   DELETE returned {code} — continuing anyway.")

# ── Step 3: Load connector config ────────────────────────────────────────────
log(f"📄  Loading connector config from {CONNECTOR_FILE} ...")
with open(CONNECTOR_FILE) as f:
    config = json.load(f)

tables = config.get("config", {}).get("table.include.list", "")
table_count = len(tables.split(",")) if tables else 0
log(f"   Connector: {config.get('name')}  |  Tables: {table_count}")

# ── Step 4: Register connector ────────────────────────────────────────────────
log("🚀  Registering connector ...")
code, resp = post("/connectors", config)

if code in (200, 201):
    log(f"✅  Connector registered successfully (HTTP {code})")
    log(f"   Name:  {resp.get('name')}")
    log(f"   Tasks: {resp.get('config', {}).get('tasks.max', '?')}")
else:
    log(f"❌  Registration failed (HTTP {code})")
    log(f"   Response: {json.dumps(resp, indent=2)}")
    sys.exit(1)

# ── Step 5: Verify status ─────────────────────────────────────────────────────
log("🔍  Verifying connector status ...")
time.sleep(5)
try:
    code, status = get(f"/connectors/{CONNECTOR_NAME}/status")
    connector_state = status.get("connector", {}).get("state", "UNKNOWN")
    task_states     = [t.get("state") for t in status.get("tasks", [])]
    log(f"   Connector state : {connector_state}")
    log(f"   Task states     : {task_states}")
    if connector_state == "RUNNING":
        log("✅  Debezium CDC connector is RUNNING — snapshot will begin shortly.")
    else:
        log(f"⚠️   Connector state is {connector_state} — check: docker logs kafka_connect")
except Exception as e:
    log(f"⚠️   Could not fetch status: {e}")

log("Done.")
