#!/usr/bin/env python3
"""
Payment Gateway — Real-Time Streaming Data Generator
=====================================================
Continuously inserts live data at controlled rates:
  • 2 transactions / second
  • 2 clients     / hour
  • 1 POS         / day

Requirements:
    pip install mysql-connector-python faker

Usage:
    python stream_data.py [--host localhost] [--port 3306]
                          [--user pg_user] [--password pg_password_2024]
                          [--database payment_gateway]

Press  Ctrl+C  to stop gracefully.
"""

import argparse
import math
import random
import signal
import sys
import threading
import time
import uuid
from datetime import datetime, timedelta

import mysql.connector
from mysql.connector import pooling
from faker import Faker

# ─────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────
parser = argparse.ArgumentParser(description="Real-Time Payment Gateway Stream")
parser.add_argument("--host",     default="localhost")
parser.add_argument("--port",     type=int, default=3306)
parser.add_argument("--user",     default="pg_user")
parser.add_argument("--password", default="pg_password_2024")
parser.add_argument("--database", default="payment_gateway")
parser.add_argument("--txn-per-second", type=float, default=2.0,
                    help="Target transactions per second (default: 2)")
parser.add_argument("--clients-per-hour", type=float, default=2.0,
                    help="New clients per hour (default: 2)")
parser.add_argument("--pos-per-day", type=float, default=1.0,
                    help="New POS terminals per day (default: 1)")
args = parser.parse_args()

fake = Faker()

# ─────────────────────────────────────────────
# Connection pool  (1 connection per worker)
# ─────────────────────────────────────────────
pool = pooling.MySQLConnectionPool(
    pool_name="pg_stream",
    pool_size=5,
    host=args.host, port=args.port,
    user=args.user, password=args.password,
    database=args.database,
    autocommit=True,
)

def get_conn():
    return pool.get_connection()

# ─────────────────────────────────────────────
# Load reference data once at startup
# ─────────────────────────────────────────────
def load_ref():
    conn = get_conn()
    cur  = conn.cursor()

    cur.execute("SELECT country_id FROM countries")
    countries = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT network_id FROM card_networks")
    networks = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT type_id FROM transaction_types")
    type_ids = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT status_id, code FROM transaction_statuses")
    rows      = cur.fetchall()
    status_map = {code: sid for sid, code in rows}
    all_sids  = [r[0] for r in rows]

    cur.execute("SELECT mcc FROM merchant_categories")
    mccs = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT merchant_id FROM merchants")
    merchants = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT pos_id, merchant_id FROM pos_terminals")
    pos_rows = cur.fetchall()
    pos_ids  = [r[0] for r in pos_rows]
    pos_merc = {r[0]: r[1] for r in pos_rows}

    cur.execute("SELECT client_id FROM clients")
    clients = [r[0] for r in cur.fetchall()]

    cur.execute("SELECT card_id, client_id FROM payment_cards")
    card_rows   = cur.fetchall()
    client_cards = {}
    for cid_card, cid_client in card_rows:
        client_cards.setdefault(cid_client, []).append(cid_card)

    cur.close()
    conn.close()
    return dict(
        countries=countries, networks=networks, type_ids=type_ids,
        status_map=status_map, all_sids=all_sids, mccs=mccs,
        merchants=merchants, pos_ids=pos_ids, pos_merc=pos_merc,
        clients=clients, client_cards=client_cards,
    )

print("🔌  Connecting & loading reference data …")
REF = load_ref()
print(f"   ✓ {len(REF['clients'])} clients | "
      f"{len(REF['pos_ids'])} POS | "
      f"{len(REF['merchants'])} merchants loaded\n")

# ─────────────────────────────────────────────
# Thread-safe counters
# ─────────────────────────────────────────────
lock    = threading.Lock()
STATS   = {"txns": 0, "clients": 0, "pos": 0, "errors": 0, "start": time.time()}
RUNNING = threading.Event()
RUNNING.set()

def inc(key, n=1):
    with lock:
        STATS[key] += n

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def rnd_amount(low=1.0, high=4000.0) -> float:
    val = math.exp(random.uniform(math.log(low), math.log(high)))
    return round(val, 2)

def calc_fee(amount: float) -> float:
    return round(amount * random.choice([0.015, 0.02, 0.025, 0.03])
                 + random.choice([0.10, 0.25, 0.50]), 2)

CHANNELS      = ["POS","ONLINE","ATM","MOBILE","IVR"]
CHAN_WEIGHTS   = [55, 25, 8, 10, 2]
CURRENCIES    = ["USD","EUR","GBP","EGP","SAR","AED","CAD","AUD"]
CUR_WEIGHTS   = [50, 15, 10, 8, 6, 5, 4, 2]
DECLINE_REASONS = [
    "Insufficient funds","Card expired","Invalid CVV",
    "Blocked card","Limit exceeded","Suspected fraud",
]
FAIL_REASONS  = ["Network timeout","Gateway error","Bank unreachable"]

def status_pool():
    sm = REF["status_map"]
    return (
        [sm.get("APPROVED",  1)] * 75 +
        [sm.get("DECLINED",  2)] * 12 +
        [sm.get("SETTLED",   6)] * 10 +
        [sm.get("PENDING",   3)] *  2 +
        [sm.get("FAILED",    5)] *  1
    )

STATUS_POOL = status_pool()

NET_PREFIX = {1: "4", 2: "51", 3: "37", 4: "60"}
POS_MODELS = ["Verifone VX520","Ingenico iCT250","PAX S300",
              "Newland N910","Sunmi P2","Castles VEGA3000"]
FIRMWARES  = ["v2.1.0","v2.3.5","v3.0.1","v3.2.0","v4.0.0"]
LOC_LABELS = ["Cashier 1","Cashier 2","Counter","Drive-Thru",
              "Self-Service","Mobile Unit","Entrance","Exit"]
FRAUD_RULES = ["VELOCITY_CHECK","HIGH_AMOUNT","UNUSUAL_LOCATION",
               "BIN_MISMATCH","MULTIPLE_DECLINES","NIGHT_TRANSACTION",
               "NEW_DEVICE","COUNTRY_BLOCK"]

# ─────────────────────────────────────────────
# Workers
# ─────────────────────────────────────────────

# ── Transaction worker ───────────────────────
def insert_transaction():
    """Insert one transaction (and optionally a fraud alert)."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        now = datetime.now()

        client_id  = random.choice(REF["clients"])
        cards      = REF["client_cards"].get(client_id, [])
        card_id    = random.choice(cards) if cards else None
        pos_id     = random.choice(REF["pos_ids"]) if random.random() < 0.70 else None
        merchant_id= REF["pos_merc"].get(pos_id) if pos_id else random.choice(REF["merchants"])
        type_id    = random.choice(REF["type_ids"])
        status_id  = random.choice(STATUS_POOL)
        amount     = rnd_amount()
        fee        = calc_fee(amount)
        currency   = random.choices(CURRENCIES, weights=CUR_WEIGHTS)[0]
        channel    = random.choices(CHANNELS, weights=CHAN_WEIGHTS)[0]
        is_intl    = 1 if currency != "USD" and random.random() < 0.3 else 0
        is_recur   = 1 if random.random() < 0.08 else 0
        is_flagged = 1 if amount > 2500 and random.random() < 0.05 else 0
        declined_sid = REF["status_map"].get("DECLINED")
        failed_sid   = REF["status_map"].get("FAILED")
        fail_rsn   = None
        if status_id == declined_sid:
            fail_rsn = random.choice(DECLINE_REASONS)
        elif status_id == failed_sid:
            fail_rsn = random.choice(FAIL_REASONS)

        approved_sid = REF["status_map"].get("APPROVED")
        auth_code    = fake.bothify("??????")[:20] if status_id == approved_sid else None
        proc_at      = now + timedelta(seconds=random.randint(1, 8))
        settled_sid  = REF["status_map"].get("SETTLED")
        settled      = (now + timedelta(days=random.randint(1,3))).date() \
                       if status_id == settled_sid else None

        cur.execute("""
            INSERT INTO transactions
              (reference, client_id, card_id, pos_id, merchant_id,
               type_id, status_id, amount, currency, fee,
               auth_code, channel, ip_address, device_id,
               is_recurring, is_international, is_flagged,
               failure_reason, initiated_at, processed_at, settled_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            str(uuid.uuid4()), client_id, card_id, pos_id, merchant_id,
            type_id, status_id, amount, currency, fee,
            auth_code, channel,
            fake.ipv4() if channel in ("ONLINE","MOBILE") else None,
            fake.md5()[:80] if channel in ("ONLINE","MOBILE") else None,
            is_recur, is_intl, is_flagged,
            fail_rsn, now, proc_at, settled,
        ))
        txn_id = cur.lastrowid

        # Fraud alert for flagged transactions
        if is_flagged:
            reviewed   = random.random() < 0.40
            confirmed  = random.choices([1, 0], weights=[20, 80])[0] if reviewed else None
            cur.execute("""
                INSERT INTO fraud_alerts
                  (txn_id, rule_triggered, risk_score, is_confirmed, reviewed_by, reviewed_at)
                VALUES (%s,%s,%s,%s,%s,%s)
            """, (
                txn_id,
                random.choice(FRAUD_RULES),
                random.randint(50, 100),
                confirmed,
                fake.name()[:60] if reviewed else None,
                datetime.now() if reviewed else None,
            ))

        inc("txns")
    except Exception as e:
        inc("errors")
    finally:
        cur.close()
        conn.close()


# ── Client worker ────────────────────────────
def insert_client():
    """Insert one new client + 1–3 payment cards."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        now = datetime.now()
        cur.execute("""
            INSERT INTO clients
              (first_name, last_name, email, phone, date_of_birth, national_id,
               country_id, city, address, zip_code, risk_score, is_active, registered_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            fake.first_name(), fake.last_name(),
            fake.unique.email(), fake.phone_number()[:30],
            fake.date_of_birth(minimum_age=18, maximum_age=75),
            fake.bothify("##########"),
            random.choice(REF["countries"]),
            fake.city(), fake.street_address(), fake.postcode(),
            random.randint(1, 100), 1, now,
        ))
        client_id = cur.lastrowid

        n_cards = random.choices([1,2,3], weights=[60,30,10])[0]
        new_card_ids = []
        for k in range(n_cards):
            net = random.choice(REF["networks"])
            pan = NET_PREFIX.get(net, "4") + fake.numerify("## **** **** ####")
            exp_y = now.year + random.randint(1, 5)
            cur.execute("""
                INSERT INTO payment_cards
                  (client_id, network_id, masked_pan, card_token, holder_name,
                   expiry_month, expiry_year, is_default, is_active, issued_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                client_id, net, pan[:19], str(uuid.uuid4()),
                fake.name()[:100],
                random.randint(1,12), exp_y,
                1 if k == 0 else 0, 1, now,
            ))
            new_card_ids.append(cur.lastrowid)

        # Register in live cache (thread-safe)
        with lock:
            REF["clients"].append(client_id)
            REF["client_cards"][client_id] = new_card_ids

        inc("clients")
    except Exception as e:
        inc("errors")
    finally:
        cur.close()
        conn.close()


# ── POS worker ───────────────────────────────
def insert_pos():
    """Insert one new POS terminal."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        now        = datetime.now()
        serial     = f"POS-LIVE-{now.strftime('%Y%m%d')}-{fake.lexify('??????').upper()}"
        merchant_id= random.choice(REF["merchants"])

        cur.execute("""
            INSERT INTO pos_terminals
              (serial_number, merchant_id, model, firmware_version,
               location_label, is_active, installed_at, last_seen_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            serial, merchant_id,
            random.choice(POS_MODELS), random.choice(FIRMWARES),
            random.choice(LOC_LABELS),
            1, now, now,
        ))
        pos_id = cur.lastrowid

        # Register in live cache
        with lock:
            REF["pos_ids"].append(pos_id)
            REF["pos_merc"][pos_id] = merchant_id

        inc("pos")
    except Exception as e:
        inc("errors")
    finally:
        cur.close()
        conn.close()


# ─────────────────────────────────────────────
# Precision ticker — fires a callback every
# `interval` seconds, self-correcting for drift
# ─────────────────────────────────────────────
class PrecisionTicker(threading.Thread):
    def __init__(self, interval: float, callback, name: str):
        super().__init__(daemon=True, name=name)
        self.interval = interval
        self.callback = callback

    def run(self):
        next_tick = time.perf_counter()
        while RUNNING.is_set():
            now = time.perf_counter()
            if now >= next_tick:
                threading.Thread(target=self.callback, daemon=True).start()
                next_tick += self.interval
            time.sleep(max(0, next_tick - time.perf_counter() - 0.001))


# ─────────────────────────────────────────────
# Status dashboard  (prints every 5 s)
# ─────────────────────────────────────────────
def print_status():
    while RUNNING.is_set():
        time.sleep(5)
        elapsed  = time.time() - STATS["start"]
        minutes  = elapsed / 60
        with lock:
            t = STATS["txns"]
            c = STATS["clients"]
            p = STATS["pos"]
            e = STATS["errors"]
        tps  = t / elapsed if elapsed else 0
        cph  = c / (elapsed / 3600) if elapsed else 0
        ppd  = p / (elapsed / 86400) if elapsed else 0

        print(
            f"\r  ⏱  {int(elapsed):>6}s elapsed │ "
            f"💰 {t:>7} txns  ({tps:5.2f}/s) │ "
            f"👤 {c:>5} clients  ({cph:5.2f}/hr) │ "
            f"🖥  {p:>4} POS  ({ppd:5.2f}/day) │ "
            f"❌ {e} errors",
            end="", flush=True,
        )


# ─────────────────────────────────────────────
# Graceful shutdown
# ─────────────────────────────────────────────
def shutdown(sig, frame):
    print("\n\n🛑  Shutting down …")
    RUNNING.clear()
    time.sleep(0.5)
    elapsed = time.time() - STATS["start"]
    print("─" * 60)
    print(f"  Total runtime      : {elapsed:.1f} s")
    print(f"  Transactions       : {STATS['txns']:,}")
    print(f"  Clients added      : {STATS['clients']:,}")
    print(f"  POS added          : {STATS['pos']:,}")
    print(f"  Errors             : {STATS['errors']:,}")
    print(f"  Avg TPS            : {STATS['txns']/elapsed:.2f}")
    print("─" * 60)
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ─────────────────────────────────────────────
# Launch tickers
# ─────────────────────────────────────────────
TXN_INTERVAL    = 1.0 / args.txn_per_second        # 0.5 s  → 2 txns/s
CLIENT_INTERVAL = 3600.0 / args.clients_per_hour    # 1800 s → 2 clients/hr
POS_INTERVAL    = 86400.0 / args.pos_per_day        # 86400 s → 1 POS/day

print("🚀  Starting real-time stream …")
print(f"   Transactions : {args.txn_per_second}/s   (every {TXN_INTERVAL:.2f}s)")
print(f"   Clients      : {args.clients_per_hour}/hr  (every {CLIENT_INTERVAL:.0f}s)")
print(f"   POS          : {args.pos_per_day}/day (every {POS_INTERVAL:.0f}s)")
print("\n   Press Ctrl+C to stop\n")

PrecisionTicker(TXN_INTERVAL,    insert_transaction, "txn-ticker").start()
PrecisionTicker(CLIENT_INTERVAL, insert_client,      "client-ticker").start()
PrecisionTicker(POS_INTERVAL,    insert_pos,         "pos-ticker").start()

# Status thread
threading.Thread(target=print_status, daemon=True, name="status").start()

# Keep main thread alive
while RUNNING.is_set():
    time.sleep(1)
