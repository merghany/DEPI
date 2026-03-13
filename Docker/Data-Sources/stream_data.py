#!/usr/bin/env python3
"""
Payment Gateway — Real-Time Streaming Data Generator
=====================================================
Continuously inserts live data at controlled rates:
  • 2 transactions   / second   (normal)
  • 2 clients        / hour
  • 1 POS            / day
  • 1 fraud campaign / hour     (random pattern, random burst size)

Fraud patterns (one chosen at random each hour):
  1. velocity_burst       — same client, rapid-fire txns within minutes
  2. card_testing         — micro-charges ($0.01–$1.99) to probe card
  3. high_value_fraud     — single large txn > $3 000 on ONLINE/MOBILE
  4. night_owl            — off-hours cluster between 01:00–04:59
  5. international_spree  — multi-currency cross-border burst
  6. account_takeover     — new device + foreign currency + high amount

Requirements:
    pip install mysql-connector-python faker

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
parser.add_argument("--host",               default="localhost")
parser.add_argument("--port",               type=int,   default=3306)
parser.add_argument("--user",               default="pg_user")
parser.add_argument("--password",           default="pg_password_2024")
parser.add_argument("--database",           default="payment_gateway")
parser.add_argument("--txn-per-second",     type=float, default=2.0)
parser.add_argument("--clients-per-hour",   type=float, default=2.0)
parser.add_argument("--pos-per-day",        type=float, default=1.0)
parser.add_argument("--fraud-per-hour",     type=float, default=1.0,
                    help="Fraud campaigns per hour (default: 1)")
args = parser.parse_args()

fake = Faker()

# ─────────────────────────────────────────────
# Connection pool
# ─────────────────────────────────────────────
pool = pooling.MySQLConnectionPool(
    pool_name="pg_stream",
    pool_size=8,                        # extra capacity for fraud bursts
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
    rows       = cur.fetchall()
    status_map = {code: sid for sid, code in rows}

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
    card_rows    = cur.fetchall()
    client_cards = {}
    for card_id, cid in card_rows:
        client_cards.setdefault(cid, []).append(card_id)

    cur.close()
    conn.close()
    return dict(
        countries=countries, networks=networks, type_ids=type_ids,
        status_map=status_map, mccs=mccs, merchants=merchants,
        pos_ids=pos_ids, pos_merc=pos_merc,
        clients=clients, client_cards=client_cards,
    )

print("🔌  Connecting & loading reference data …")
REF = load_ref()
print(f"   ✓ {len(REF['clients']):,} clients | "
      f"{len(REF['pos_ids']):,} POS | "
      f"{len(REF['merchants']):,} merchants loaded\n")

# ─────────────────────────────────────────────
# Thread-safe counters
# ─────────────────────────────────────────────
lock  = threading.Lock()
STATS = {
    "txns": 0, "clients": 0, "pos": 0,
    "fraud_campaigns": 0, "fraud_txns": 0,
    "errors": 0, "start": time.time(),
}
RUNNING = threading.Event()
RUNNING.set()

def inc(key, n=1):
    with lock:
        STATS[key] += n

# ─────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────
def rnd_amount(low=1.0, high=4000.0) -> float:
    return round(math.exp(random.uniform(math.log(low), math.log(high))), 2)

def calc_fee(amount: float) -> float:
    return round(amount * random.choice([0.015, 0.02, 0.025, 0.03])
                 + random.choice([0.10, 0.25, 0.50]), 2)

CHANNELS       = ["POS","ONLINE","ATM","MOBILE","IVR"]
CHAN_WEIGHTS    = [55, 25, 8, 10, 2]
CURRENCIES     = ["USD","EUR","GBP","EGP","SAR","AED","CAD","AUD"]
CUR_WEIGHTS    = [50, 15, 10, 8, 6, 5, 4, 2]
FOREIGN_CURR   = [c for c in CURRENCIES if c != "USD"]
DECLINE_RSN    = ["Insufficient funds","Card expired","Invalid CVV",
                  "Blocked card","Limit exceeded","Suspected fraud"]
FAIL_RSN       = ["Network timeout","Gateway error","Bank unreachable"]

NET_PREFIX = {1: "4", 2: "51", 3: "37", 4: "60"}
POS_MODELS = ["Verifone VX520","Ingenico iCT250","PAX S300",
              "Newland N910","Sunmi P2","Castles VEGA3000"]
FIRMWARES  = ["v2.1.0","v2.3.5","v3.0.1","v3.2.0","v4.0.0"]
LOC_LABELS = ["Cashier 1","Cashier 2","Counter","Drive-Thru",
              "Self-Service","Mobile Unit","Entrance","Exit"]

# Rule catalogue per fraud type
FRAUD_RULE_MAP = {
    "velocity_burst":      ["VELOCITY_CHECK", "RAPID_SUCCESSION", "SAME_MERCHANT_REPEAT"],
    "card_testing":        ["CARD_TESTING", "MICRO_AMOUNT", "BIN_MISMATCH"],
    "high_value_fraud":    ["HIGH_AMOUNT", "RISK_SCORE_BREACH", "LARGE_TXN_ONLINE"],
    "night_owl":           ["NIGHT_TRANSACTION", "OFF_HOURS", "UNUSUAL_LOCATION"],
    "international_spree": ["CROSS_BORDER", "CURRENCY_MISMATCH", "COUNTRY_BLOCK"],
    "account_takeover":    ["NEW_DEVICE", "IP_MISMATCH", "BROWSER_FINGERPRINT"],
}

def _status_pool():
    sm = REF["status_map"]
    return (
        [sm.get("APPROVED", 1)] * 73 +
        [sm.get("DECLINED", 2)] * 14 +
        [sm.get("SETTLED",  6)] *  8 +
        [sm.get("PENDING",  3)] *  3 +
        [sm.get("FAILED",   5)] *  2
    )

STATUS_POOL = _status_pool()

# ─────────────────────────────────────────────
# Low-level transaction builder
# ─────────────────────────────────────────────
TXN_SQL = """
    INSERT INTO transactions
      (reference, client_id, card_id, pos_id, merchant_id,
       type_id, status_id, amount, currency, fee,
       auth_code, channel, ip_address, device_id,
       is_recurring, is_international, is_flagged,
       failure_reason, initiated_at, processed_at, settled_at)
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

ALERT_SQL = """
    INSERT INTO fraud_alerts
      (txn_id, rule_triggered, risk_score, is_confirmed, reviewed_by, reviewed_at)
    VALUES (%s,%s,%s,%s,%s,%s)
"""

def _build_txn(
    client_id=None, card_id=None, pos_id=None, merchant_id=None,
    amount=None, currency=None, channel=None,
    status_id=None, type_id=None,
    is_flagged=0, is_international=0, is_recurring=0,
    initiated_at=None, ip_address=None, device_id=None,
):
    """Assemble one transaction row tuple with sensible defaults."""
    now = datetime.now()

    if client_id   is None: client_id   = random.choice(REF["clients"])
    if card_id     is None:
        cards   = REF["client_cards"].get(client_id, [])
        card_id = random.choice(cards) if cards else None
    if pos_id      is None:
        pos_id  = random.choice(REF["pos_ids"]) if random.random() < 0.70 else None
    if merchant_id is None:
        merchant_id = REF["pos_merc"].get(pos_id) if pos_id else random.choice(REF["merchants"])
    if amount      is None: amount      = rnd_amount()
    if currency    is None: currency    = random.choices(CURRENCIES, weights=CUR_WEIGHTS)[0]
    if channel     is None: channel     = random.choices(CHANNELS, weights=CHAN_WEIGHTS)[0]
    if status_id   is None: status_id   = random.choice(STATUS_POOL)
    if type_id     is None: type_id     = random.choice(REF["type_ids"])
    if initiated_at is None: initiated_at = now

    approved_sid = REF["status_map"].get("APPROVED")
    declined_sid = REF["status_map"].get("DECLINED")
    failed_sid   = REF["status_map"].get("FAILED")
    settled_sid  = REF["status_map"].get("SETTLED")

    fee      = calc_fee(amount)
    proc_at  = initiated_at + timedelta(seconds=random.randint(1, 8))
    settled  = (initiated_at + timedelta(days=random.randint(1, 3))).date() \
               if status_id == settled_sid else None

    fail_rsn = None
    if status_id == declined_sid: fail_rsn = random.choice(DECLINE_RSN)
    elif status_id == failed_sid: fail_rsn = random.choice(FAIL_RSN)

    auth_code = fake.bothify("??????")[:20] if status_id == approved_sid else None
    ip        = ip_address or (fake.ipv4() if channel in ("ONLINE","MOBILE") else None)
    dev       = device_id  or (fake.md5()[:80] if channel in ("ONLINE","MOBILE") else None)

    return (
        str(uuid.uuid4()), client_id, card_id, pos_id, merchant_id,
        type_id, status_id, amount, currency, fee,
        auth_code, channel, ip, dev,
        is_recurring, is_international, is_flagged,
        fail_rsn, initiated_at, proc_at, settled,
    )

def _insert_alert(cur, txn_id, pattern_name, amount):
    """Insert 1-3 fraud alert rules for a flagged transaction."""
    rules    = FRAUD_RULE_MAP.get(pattern_name, ["VELOCITY_CHECK","HIGH_AMOUNT"])
    n_rules  = random.choices([1, 2, 3], weights=[40, 40, 20])[0]
    chosen   = random.sample(rules, min(n_rules, len(rules)))
    base_risk = 60 if amount < 1000 else 75 if amount < 3000 else 90
    for rule in chosen:
        reviewed  = random.random() < 0.70
        confirmed = random.choices([1, 0], weights=[40, 60])[0] if reviewed else None
        cur.execute(ALERT_SQL, (
            txn_id,
            rule,
            min(100, base_risk + random.randint(-5, 10)),
            confirmed,
            fake.name()[:60] if reviewed else None,
            datetime.now()   if reviewed else None,
        ))

# ─────────────────────────────────────────────
# Fraud pattern functions
# Each returns (pattern_name, list_of_txn_rows)
# ─────────────────────────────────────────────

def _fraud_velocity_burst():
    """5-20 rapid transactions from the same client within a 10-minute window."""
    rows      = []
    client_id = random.choice(REF["clients"])
    cards     = REF["client_cards"].get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    t0        = datetime.now()
    n         = random.randint(5, 20)
    approved  = REF["status_map"].get("APPROVED")
    declined  = REF["status_map"].get("DECLINED")
    shared_ip = fake.ipv4()
    shared_dev= fake.md5()[:80]
    for i in range(n):
        t = t0 + timedelta(seconds=i * random.randint(10, 60))
        rows.append(_build_txn(
            client_id=client_id, card_id=card_id,
            amount=rnd_amount(50, 500),
            channel="ONLINE",
            status_id=random.choices([approved, declined], weights=[60, 40])[0],
            is_flagged=1,
            initiated_at=t,
            ip_address=shared_ip,
            device_id=shared_dev,
        ))
    return "velocity_burst", rows


def _fraud_card_testing():
    """10-30 micro-charges ($0.01–$1.99) probing whether a card is live."""
    rows      = []
    client_id = random.choice(REF["clients"])
    cards     = REF["client_cards"].get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    t0        = datetime.now()
    n         = random.randint(10, 30)
    approved  = REF["status_map"].get("APPROVED")
    declined  = REF["status_map"].get("DECLINED")
    for i in range(n):
        t = t0 + timedelta(seconds=i * random.randint(5, 30))
        rows.append(_build_txn(
            client_id=client_id, card_id=card_id,
            amount=round(random.uniform(0.01, 1.99), 2),
            channel="ONLINE",
            status_id=random.choices([approved, declined], weights=[50, 50])[0],
            is_flagged=1,
            initiated_at=t,
        ))
    return "card_testing", rows


def _fraud_high_value():
    """Single large transaction ($3 000–$9 999) on ONLINE or MOBILE."""
    amount  = rnd_amount(3_000, 9_999)
    channel = random.choice(["ONLINE", "MOBILE"])
    approved= REF["status_map"].get("APPROVED")
    declined= REF["status_map"].get("DECLINED")
    rows = [_build_txn(
        amount=amount,
        currency=random.choice(FOREIGN_CURR),
        channel=channel,
        status_id=random.choices([approved, declined], weights=[55, 45])[0],
        is_flagged=1,
        is_international=1,
    )]
    return "high_value_fraud", rows


def _fraud_night_owl():
    """2-8 off-hours transactions stamped between 01:00 and 04:59 AM."""
    rows     = []
    client_id= random.choice(REF["clients"])
    n        = random.randint(2, 8)
    now      = datetime.now()
    base     = now.replace(
        hour=random.randint(1, 4),
        minute=random.randint(0, 59),
        second=random.randint(0, 59),
    )
    # If that time is in the future today, shift to yesterday
    if base > now:
        base -= timedelta(days=1)
    for i in range(n):
        t = base + timedelta(minutes=i * random.randint(5, 20))
        rows.append(_build_txn(
            client_id=client_id,
            amount=rnd_amount(100, 3_000),
            channel=random.choice(["ONLINE", "MOBILE"]),
            is_flagged=1,
            initiated_at=t,
        ))
    return "night_owl", rows


def _fraud_international_spree():
    """3-10 rapid cross-border transactions in multiple foreign currencies."""
    rows      = []
    client_id = random.choice(REF["clients"])
    cards     = REF["client_cards"].get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    t0        = datetime.now()
    n         = random.randint(3, 10)
    for i in range(n):
        t = t0 + timedelta(minutes=i * random.randint(1, 15))
        rows.append(_build_txn(
            client_id=client_id, card_id=card_id,
            amount=rnd_amount(200, 4_000),
            currency=random.choice(FOREIGN_CURR),
            channel="ONLINE",
            is_flagged=1,
            is_international=1,
            initiated_at=t,
        ))
    return "international_spree", rows


def _fraud_account_takeover():
    """New device + foreign currency + high amount — classic ATO signal."""
    approved= REF["status_map"].get("APPROVED")
    declined= REF["status_map"].get("DECLINED")
    rows = [_build_txn(
        amount=rnd_amount(500, 5_000),
        currency=random.choice(FOREIGN_CURR),
        channel="ONLINE",
        status_id=random.choices([approved, declined], weights=[65, 35])[0],
        is_flagged=1,
        is_international=random.choice([0, 1]),
        ip_address=fake.ipv4(),
        device_id=fake.md5()[:80],
    )]
    return "account_takeover", rows


FRAUD_PATTERNS = [
    _fraud_velocity_burst,
    _fraud_card_testing,
    _fraud_high_value,
    _fraud_night_owl,
    _fraud_international_spree,
    _fraud_account_takeover,
]

PATTERN_LABELS = {
    "velocity_burst":      "💨 Velocity Burst",
    "card_testing":        "🔍 Card Testing",
    "high_value_fraud":    "💸 High-Value Fraud",
    "night_owl":           "🌙 Night Owl",
    "international_spree": "🌍 International Spree",
    "account_takeover":    "🔓 Account Takeover",
}

# ─────────────────────────────────────────────
# Workers
# ─────────────────────────────────────────────

def insert_transaction():
    """Insert one normal transaction (and optionally a low-level fraud alert)."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        now          = datetime.now()
        client_id    = random.choice(REF["clients"])
        cards        = REF["client_cards"].get(client_id, [])
        card_id      = random.choice(cards) if cards else None
        pos_id       = random.choice(REF["pos_ids"]) if random.random() < 0.70 else None
        merchant_id  = REF["pos_merc"].get(pos_id) if pos_id else random.choice(REF["merchants"])
        amount       = rnd_amount()
        currency     = random.choices(CURRENCIES, weights=CUR_WEIGHTS)[0]
        channel      = random.choices(CHANNELS, weights=CHAN_WEIGHTS)[0]
        status_id    = random.choice(STATUS_POOL)
        type_id      = random.choice(REF["type_ids"])
        is_intl      = 1 if currency != "USD" and random.random() < 0.3 else 0
        is_recur     = 1 if random.random() < 0.08 else 0
        is_flagged   = 1 if amount > 2500 and random.random() < 0.03 else 0

        row = _build_txn(
            client_id=client_id, card_id=card_id,
            pos_id=pos_id, merchant_id=merchant_id,
            amount=amount, currency=currency, channel=channel,
            status_id=status_id, type_id=type_id,
            is_flagged=is_flagged, is_international=is_intl, is_recurring=is_recur,
            initiated_at=now,
        )
        cur.execute(TXN_SQL, row)
        txn_id = cur.lastrowid

        if is_flagged:
            _insert_alert(cur, txn_id, "high_value_fraud", amount)

        inc("txns")
    except Exception:
        inc("errors")
    finally:
        cur.close()
        conn.close()


def insert_fraud_campaign():
    """
    Pick a random fraud pattern, insert its full burst of transactions
    plus fraud alerts, all in one DB round-trip.
    """
    pattern_fn = random.choice(FRAUD_PATTERNS)
    conn = get_conn()
    cur  = conn.cursor()
    try:
        pattern_name, txn_rows = pattern_fn()
        txn_ids = []

        for row in txn_rows:
            cur.execute(TXN_SQL, row)
            txn_ids.append(cur.lastrowid)

        # Insert fraud alerts for every transaction in the campaign
        for txn_id, row in zip(txn_ids, txn_rows):
            amount = row[7]    # amount is index 7 in the tuple
            _insert_alert(cur, txn_id, pattern_name, float(amount))

        label = PATTERN_LABELS.get(pattern_name, pattern_name)
        n     = len(txn_rows)

        inc("fraud_campaigns")
        inc("fraud_txns", n)
        inc("txns", n)

        # Print on its own line so it doesn't smash the status bar
        print(f"\n  🚨  Fraud campaign fired │ {label:<28} │ {n:>3} txns", flush=True)

    except Exception as e:
        inc("errors")
    finally:
        cur.close()
        conn.close()


def insert_client():
    """Insert one new client + 1-3 payment cards."""
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

        new_card_ids = []
        for k in range(random.choices([1, 2, 3], weights=[60, 30, 10])[0]):
            net = random.choice(REF["networks"])
            pan = NET_PREFIX.get(net, "4") + fake.numerify("## **** **** ####")
            cur.execute("""
                INSERT INTO payment_cards
                  (client_id, network_id, masked_pan, card_token, holder_name,
                   expiry_month, expiry_year, is_default, is_active, issued_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                client_id, net, pan[:19], str(uuid.uuid4()),
                fake.name()[:100],
                random.randint(1, 12), now.year + random.randint(1, 5),
                1 if k == 0 else 0, 1, now,
            ))
            new_card_ids.append(cur.lastrowid)

        with lock:
            REF["clients"].append(client_id)
            REF["client_cards"][client_id] = new_card_ids

        inc("clients")
    except Exception:
        inc("errors")
    finally:
        cur.close()
        conn.close()


def insert_pos():
    """Insert one new POS terminal."""
    conn = get_conn()
    cur  = conn.cursor()
    try:
        now         = datetime.now()
        serial      = f"POS-LIVE-{now.strftime('%Y%m%d')}-{fake.lexify('??????').upper()}"
        merchant_id = random.choice(REF["merchants"])
        cur.execute("""
            INSERT INTO pos_terminals
              (serial_number, merchant_id, model, firmware_version,
               location_label, is_active, installed_at, last_seen_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            serial, merchant_id,
            random.choice(POS_MODELS), random.choice(FIRMWARES),
            random.choice(LOC_LABELS), 1, now, now,
        ))
        pos_id = cur.lastrowid

        with lock:
            REF["pos_ids"].append(pos_id)
            REF["pos_merc"][pos_id] = merchant_id

        inc("pos")
    except Exception:
        inc("errors")
    finally:
        cur.close()
        conn.close()


# ─────────────────────────────────────────────
# Precision ticker
# ─────────────────────────────────────────────
class PrecisionTicker(threading.Thread):
    def __init__(self, interval: float, callback, name: str):
        super().__init__(daemon=True, name=name)
        self.interval = interval
        self.callback = callback

    def run(self):
        next_tick = time.perf_counter()
        while RUNNING.is_set():
            if time.perf_counter() >= next_tick:
                threading.Thread(target=self.callback, daemon=True).start()
                next_tick += self.interval
            time.sleep(max(0, next_tick - time.perf_counter() - 0.001))


# ─────────────────────────────────────────────
# Status dashboard (every 5 s)
# ─────────────────────────────────────────────
def print_status():
    while RUNNING.is_set():
        time.sleep(5)
        elapsed = time.time() - STATS["start"]
        with lock:
            t  = STATS["txns"]
            c  = STATS["clients"]
            p  = STATS["pos"]
            fc = STATS["fraud_campaigns"]
            ft = STATS["fraud_txns"]
            e  = STATS["errors"]
        tps = t / elapsed if elapsed else 0
        cph = c / (elapsed / 3600)  if elapsed else 0
        ppd = p / (elapsed / 86400) if elapsed else 0
        fph = fc / (elapsed / 3600) if elapsed else 0
        print(
            f"\r  ⏱ {int(elapsed):>6}s │ "
            f"💰 {t:>7} txns ({tps:5.2f}/s) │ "
            f"👤 {c:>5} clients ({cph:4.2f}/hr) │ "
            f"🖥  {p:>4} POS ({ppd:4.2f}/day) │ "
            f"🚨 {fc:>4} campaigns ({ft:,} fraud txns, {fph:.2f}/hr) │ "
            f"❌ {e} err",
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
    print("─" * 65)
    print(f"  Total runtime        : {elapsed:.1f} s")
    print(f"  Total transactions   : {STATS['txns']:,}")
    print(f"  Fraud campaigns      : {STATS['fraud_campaigns']:,}")
    print(f"  Fraud transactions   : {STATS['fraud_txns']:,}")
    print(f"  Clients added        : {STATS['clients']:,}")
    print(f"  POS added            : {STATS['pos']:,}")
    print(f"  Errors               : {STATS['errors']:,}")
    print(f"  Avg TPS              : {STATS['txns']/elapsed:.2f}")
    print("─" * 65)
    sys.exit(0)

signal.signal(signal.SIGINT,  shutdown)
signal.signal(signal.SIGTERM, shutdown)

# ─────────────────────────────────────────────
# Launch all tickers
# ─────────────────────────────────────────────
TXN_INTERVAL    = 1.0   / args.txn_per_second
CLIENT_INTERVAL = 3600.0 / args.clients_per_hour
POS_INTERVAL    = 86400.0 / args.pos_per_day
FRAUD_INTERVAL  = 3600.0 / args.fraud_per_hour

print("🚀  Starting real-time stream …")
print(f"   Normal transactions : {args.txn_per_second}/s      (every {TXN_INTERVAL:.2f}s)")
print(f"   Fraud campaigns     : {args.fraud_per_hour}/hr     (every {FRAUD_INTERVAL:.0f}s)")
print(f"   Clients             : {args.clients_per_hour}/hr   (every {CLIENT_INTERVAL:.0f}s)")
print(f"   POS terminals       : {args.pos_per_day}/day  (every {POS_INTERVAL:.0f}s)")
print("\n   Press Ctrl+C to stop\n")

PrecisionTicker(TXN_INTERVAL,    insert_transaction,    "txn-ticker").start()
PrecisionTicker(FRAUD_INTERVAL,  insert_fraud_campaign, "fraud-ticker").start()
PrecisionTicker(CLIENT_INTERVAL, insert_client,         "client-ticker").start()
PrecisionTicker(POS_INTERVAL,    insert_pos,            "pos-ticker").start()

threading.Thread(target=print_status, daemon=True, name="status").start()

while RUNNING.is_set():
    time.sleep(1)
