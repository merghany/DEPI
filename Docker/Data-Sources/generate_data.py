#!/usr/bin/env python3
"""
Electronic Payment Gateway — Sample Data Generator
====================================================
Generates:  10 000 clients | 1 000 POS terminals | 150 000+ transactions
Period   :  last 365 days back from today

Fraud patterns generated:
  1. Velocity burst      — same client, 5-20 rapid txns within minutes
  2. Card testing        — tiny amounts ($0.01-$1.99) in quick succession
  3. High-value fraud    — single large txn > $3 000 on unusual channel
  4. Night owl           — 01:00-04:59 AM transactions
  5. International spree — sudden cross-border activity
  6. Account takeover    — new device + new country + high amount

Guard: exits immediately (code 0) if transactions table already has ≥ 1 000 rows.

Requirements:
    pip install mysql-connector-python faker tqdm

Usage:
    python generate_data.py [--host localhost] [--port 3306]
                            [--user pg_user] [--password pg_password_2024]
                            [--database payment_gateway]
"""

import argparse
import random
import uuid
import math
import sys
from datetime import datetime, timedelta
from decimal import Decimal

import mysql.connector
from faker import Faker
from tqdm import tqdm

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Payment Gateway Data Generator")
parser.add_argument("--host",         default="localhost")
parser.add_argument("--port",         type=int, default=3306)
parser.add_argument("--user",         default="pg_user")
parser.add_argument("--password",     default="pg_password_2024")
parser.add_argument("--database",     default="payment_gateway")
parser.add_argument("--clients",      type=int, default=10_000)
parser.add_argument("--pos",          type=int, default=1_000)
parser.add_argument("--transactions", type=int, default=155_000)
parser.add_argument("--min-txn-threshold", type=int, default=1_000,
                    help="Skip generation if DB already has this many transactions (default: 1000)")
args = parser.parse_args()

fake = Faker()
Faker.seed(42)
random.seed(42)

# ---------------------------------------------------------------------------
# DB connection
# ---------------------------------------------------------------------------
print("🔌  Connecting to MySQL …")
conn = mysql.connector.connect(
    host=args.host, port=args.port,
    user=args.user, password=args.password,
    database=args.database,
    autocommit=False,
)
cur = conn.cursor()

# ---------------------------------------------------------------------------
# GUARD: skip if already seeded
# ---------------------------------------------------------------------------
cur.execute("SELECT COUNT(*) FROM transactions")
existing_txns = cur.fetchone()[0]

if existing_txns >= args.min_txn_threshold:
    print(f"\n⏭️   SKIPPED — database already contains {existing_txns:,} transactions "
          f"(threshold: {args.min_txn_threshold:,}).")
    print("    To re-seed from scratch: docker-compose down -v && docker-compose up --build")
    cur.close()
    conn.close()
    sys.exit(0)

print(f"   ✓ {existing_txns} existing transactions — proceeding with generation.\n")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
NOW        = datetime.now()
YEAR_AGO   = NOW - timedelta(days=365)
BATCH_SIZE = 2_000

def rnd_dt(start: datetime = YEAR_AGO, end: datetime = NOW) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def rnd_amount(low=1.0, high=5000.0) -> float:
    val = math.exp(random.uniform(math.log(low), math.log(high)))
    return round(val, 2)

def calc_fee(amount: float) -> float:
    pct  = random.choice([0.015, 0.02, 0.025, 0.03])
    flat = random.choice([0.10, 0.25, 0.50])
    return round(amount * pct + flat, 2)

def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

# ---------------------------------------------------------------------------
# Fetch reference data
# ---------------------------------------------------------------------------
cur.execute("SELECT country_id FROM countries")
country_ids = [r[0] for r in cur.fetchall()]

cur.execute("SELECT type_id FROM transaction_types")
type_ids = [r[0] for r in cur.fetchall()]

cur.execute("SELECT status_id, code FROM transaction_statuses")
status_rows  = cur.fetchall()
status_ids   = [r[0] for r in status_rows]
approved_sid = next(r[0] for r in status_rows if r[1] == "APPROVED")
declined_sid = next(r[0] for r in status_rows if r[1] == "DECLINED")
pending_sid  = next(r[0] for r in status_rows if r[1] == "PENDING")
failed_sid   = next(r[0] for r in status_rows if r[1] == "FAILED")
settled_sid  = next(r[0] for r in status_rows if r[1] == "SETTLED")

cur.execute("SELECT network_id FROM card_networks")
network_ids = [r[0] for r in cur.fetchall()]

cur.execute("SELECT mcc FROM merchant_categories")
mcc_list = [r[0] for r in cur.fetchall()]

# ---------------------------------------------------------------------------
# Validate reference tables
# ---------------------------------------------------------------------------
ref_checks = {
    "countries":            country_ids,
    "transaction_types":    type_ids,
    "transaction_statuses": status_ids,
    "card_networks":        network_ids,
    "merchant_categories":  mcc_list,
}
empty = [name for name, lst in ref_checks.items() if not lst]
if empty:
    print(f"\n❌  ERROR: The following reference tables are empty: {', '.join(empty)}")
    print("    This usually means init.sql failed partway through (e.g. a duplicate")
    print("    primary key). To fix:")
    print("      1. docker-compose down -v")
    print("      2. docker-compose up --build")
    sys.exit(1)

print(f"   ✓ Reference data loaded — "
      f"{len(country_ids)} countries, {len(type_ids)} types, "
      f"{len(status_ids)} statuses, {len(network_ids)} networks, "
      f"{len(mcc_list)} MCC codes")

# ---------------------------------------------------------------------------
# 1. MERCHANTS
# ---------------------------------------------------------------------------
print("\n📦  Inserting merchants …")
MERCHANT_COUNT = 200
merchant_sql = """
INSERT INTO merchants
  (legal_name, trade_name, mcc, tax_id, country_id, city, address,
   zip_code, email, phone, is_active, onboarded_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
merchant_rows = []
for _ in range(MERCHANT_COUNT):
    company = fake.company()
    onboard = rnd_dt(YEAR_AGO - timedelta(days=365), YEAR_AGO)
    merchant_rows.append((
        company,
        company + " " + fake.company_suffix(),
        random.choice(mcc_list),
        fake.ein(),
        random.choice(country_ids),
        fake.city(),
        fake.street_address(),
        fake.postcode(),
        fake.company_email(),
        fake.phone_number()[:30],
        random.choices([1, 0], weights=[95, 5])[0],
        onboard,
    ))

for chunk in chunks(merchant_rows, BATCH_SIZE):
    cur.executemany(merchant_sql, chunk)
conn.commit()

cur.execute("SELECT merchant_id FROM merchants")
merchant_ids = [r[0] for r in cur.fetchall()]
print(f"   ✓ {len(merchant_ids)} merchants")

# ---------------------------------------------------------------------------
# 2. POS TERMINALS
# ---------------------------------------------------------------------------
print("🖥️   Inserting POS terminals …")
pos_sql = """
INSERT INTO pos_terminals
  (serial_number, merchant_id, model, firmware_version,
   location_label, is_active, installed_at, last_seen_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""
models    = ["Verifone VX520","Ingenico iCT250","PAX S300",
             "Newland N910","Sunmi P2","Castles VEGA3000"]
firmwares = ["v2.1.0","v2.3.5","v3.0.1","v3.2.0","v4.0.0"]
labels    = ["Cashier 1","Cashier 2","Counter","Drive-Thru",
             "Self-Service","Mobile Unit","Entrance","Exit"]

pos_rows = []
for i in range(args.pos):
    install = rnd_dt(YEAR_AGO - timedelta(days=180), YEAR_AGO)
    pos_rows.append((
        f"POS-{str(i+1).zfill(6)}-{fake.lexify('????').upper()}",
        random.choice(merchant_ids),
        random.choice(models),
        random.choice(firmwares),
        random.choice(labels),
        random.choices([1, 0], weights=[97, 3])[0],
        install,
        rnd_dt(NOW - timedelta(days=2), NOW),
    ))

for chunk in chunks(pos_rows, BATCH_SIZE):
    cur.executemany(pos_sql, chunk)
conn.commit()

cur.execute("SELECT pos_id, merchant_id FROM pos_terminals")
pos_rows_db  = cur.fetchall()
pos_ids      = [r[0] for r in pos_rows_db]
pos_merchant = {r[0]: r[1] for r in pos_rows_db}
print(f"   ✓ {len(pos_ids)} POS terminals")

# ---------------------------------------------------------------------------
# 3. CLIENTS
# ---------------------------------------------------------------------------
print("👤  Inserting clients …")
client_sql = """
INSERT INTO clients
  (first_name, last_name, email, phone, date_of_birth, national_id,
   country_id, city, address, zip_code, risk_score, is_active, registered_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
client_rows = []
for _ in range(args.clients):
    reg = rnd_dt(YEAR_AGO - timedelta(days=730), YEAR_AGO)
    client_rows.append((
        fake.first_name(), fake.last_name(),
        fake.unique.email(), fake.phone_number()[:30],
        fake.date_of_birth(minimum_age=18, maximum_age=75),
        fake.bothify("##########"),
        random.choice(country_ids),
        fake.city(), fake.street_address(), fake.postcode(),
        random.randint(1, 100),
        random.choices([1, 0], weights=[96, 4])[0],
        reg,
    ))

for chunk in tqdm(list(chunks(client_rows, BATCH_SIZE)), desc="   clients"):
    cur.executemany(client_sql, chunk)
conn.commit()

cur.execute("SELECT client_id FROM clients")
client_ids = [r[0] for r in cur.fetchall()]
print(f"   ✓ {len(client_ids)} clients")

# ---------------------------------------------------------------------------
# 4. PAYMENT CARDS
# ---------------------------------------------------------------------------
print("💳  Inserting payment cards …")
card_sql = """
INSERT INTO payment_cards
  (client_id, network_id, masked_pan, card_token, holder_name,
   expiry_month, expiry_year, is_default, is_active, issued_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
net_prefix = {1: "4", 2: "51", 3: "37", 4: "60"}
card_rows  = []

for cid in client_ids:
    n_cards = random.choices([1, 2, 3, 4], weights=[50, 30, 15, 5])[0]
    for k in range(n_cards):
        net   = random.choice(network_ids)
        pan   = net_prefix.get(net, "4") + fake.numerify("## **** **** ####")
        exp_y = random.randint(NOW.year, NOW.year + 5)
        card_rows.append((
            cid, net, pan[:19], str(uuid.uuid4()),
            fake.name()[:100],
            random.randint(1, 12), exp_y,
            1 if k == 0 else 0, 1,
            rnd_dt(YEAR_AGO - timedelta(days=365), YEAR_AGO),
        ))

for chunk in tqdm(list(chunks(card_rows, BATCH_SIZE)), desc="   cards"):
    cur.executemany(card_sql, chunk)
conn.commit()

cur.execute("SELECT card_id, client_id FROM payment_cards")
card_rows_db    = cur.fetchall()
client_card_map = {}
for card_id, cid in card_rows_db:
    client_card_map.setdefault(cid, []).append(card_id)
all_card_ids = [r[0] for r in card_rows_db]
print(f"   ✓ {len(all_card_ids)} payment cards")

# ---------------------------------------------------------------------------
# 5. TRANSACTIONS  — normal + 6 fraud patterns
# ---------------------------------------------------------------------------
print("\n💰  Inserting transactions …")

CHANNELS     = ["POS", "ONLINE", "ATM", "MOBILE", "IVR"]
CHAN_WEIGHTS  = [55, 25, 8, 10, 2]
CURRENCIES   = ["USD","EUR","GBP","EGP","SAR","AED","CAD","AUD"]
CUR_WEIGHTS  = [50,  15,  10,  8,   6,   5,   4,   2]
DECLINE_RSN  = ["Insufficient funds","Card expired","Invalid CVV",
                "Blocked card","Limit exceeded","Suspected fraud"]
FAIL_RSN     = ["Network timeout","Gateway error","Bank unreachable"]

# Status pool: mostly approved
stat_pool = (
    [approved_sid] * 73 +
    [declined_sid] * 14 +
    [settled_sid]  *  8 +
    [pending_sid]  *  3 +
    [failed_sid]   *  2
)

txn_sql = """
INSERT INTO transactions
  (reference, client_id, card_id, pos_id, merchant_id,
   type_id, status_id, amount, currency, fee,
   auth_code, channel, ip_address, device_id,
   is_recurring, is_international, is_flagged,
   failure_reason, initiated_at, processed_at, settled_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""

# ── Fraud pattern builders ───────────────────────────────────────────────────

def make_txn_row(
    client_id=None, card_id=None, pos_id=None, merchant_id=None,
    amount=None, currency=None, channel=None,
    status_id=None, type_id=None,
    is_flagged=0, is_international=0, is_recurring=0,
    initiated_at=None, failure_reason=None,
    ip_address=None, device_id=None,
):
    """Build one transaction tuple with sensible defaults for any unset field."""
    if client_id  is None: client_id   = random.choice(client_ids)
    if card_id    is None:
        cards   = client_card_map.get(client_id, [])
        card_id = random.choice(cards) if cards else None
    if pos_id     is None:
        pos_id  = random.choice(pos_ids) if random.random() < 0.70 else None
    if merchant_id is None:
        merchant_id = pos_merchant.get(pos_id) if pos_id else random.choice(merchant_ids)
    if amount     is None: amount      = rnd_amount(1.0, 4000.0)
    if currency   is None: currency    = random.choices(CURRENCIES, weights=CUR_WEIGHTS)[0]
    if channel    is None: channel     = random.choices(CHANNELS, weights=CHAN_WEIGHTS)[0]
    if status_id  is None: status_id   = random.choice(stat_pool)
    if type_id    is None: type_id     = random.choice(type_ids)
    if initiated_at is None: initiated_at = rnd_dt()

    fee      = calc_fee(amount)
    proc_at  = initiated_at + timedelta(seconds=random.randint(1, 8))
    settled  = (initiated_at + timedelta(days=random.randint(1, 3))).date() \
               if status_id == settled_sid else None

    if failure_reason is None:
        if status_id == declined_sid: failure_reason = random.choice(DECLINE_RSN)
        elif status_id == failed_sid: failure_reason = random.choice(FAIL_RSN)

    auth_code = fake.bothify("??????")[:20] if status_id == approved_sid else None
    ip        = ip_address or (fake.ipv4() if channel in ("ONLINE","MOBILE") else None)
    dev       = device_id  or (fake.md5()[:80] if channel in ("ONLINE","MOBILE") else None)

    return (
        str(uuid.uuid4()), client_id, card_id, pos_id, merchant_id,
        type_id, status_id, amount, currency, fee,
        auth_code, channel, ip, dev,
        is_recurring, is_international, is_flagged,
        failure_reason, initiated_at, proc_at, settled,
    )


def pattern_velocity_burst():
    """5-20 rapid transactions from the same client within a 10-minute window."""
    rows      = []
    client_id = random.choice(client_ids)
    cards     = client_card_map.get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    burst_start = rnd_dt()
    n = random.randint(5, 20)
    for i in range(n):
        t = burst_start + timedelta(seconds=i * random.randint(10, 60))
        rows.append(make_txn_row(
            client_id=client_id, card_id=card_id,
            amount=rnd_amount(50, 500),
            channel="ONLINE",
            status_id=random.choices(
                [approved_sid, declined_sid], weights=[60, 40])[0],
            is_flagged=1,
            initiated_at=t,
            ip_address=fake.ipv4(),          # same IP across burst
            device_id=fake.md5()[:80],
        ))
    return rows


def pattern_card_testing():
    """10-30 tiny amounts ($0.01-$1.99) to probe if card is live."""
    rows      = []
    client_id = random.choice(client_ids)
    cards     = client_card_map.get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    start     = rnd_dt()
    n         = random.randint(10, 30)
    for i in range(n):
        t = start + timedelta(seconds=i * random.randint(5, 30))
        rows.append(make_txn_row(
            client_id=client_id, card_id=card_id,
            amount=round(random.uniform(0.01, 1.99), 2),
            channel="ONLINE",
            status_id=random.choices(
                [approved_sid, declined_sid], weights=[50, 50])[0],
            is_flagged=1,
            initiated_at=t,
        ))
    return rows


def pattern_high_value_fraud():
    """Single large transaction (> $3 000) from an unusual channel."""
    channel = random.choice(["ONLINE","MOBILE"])
    amount  = rnd_amount(3_000, 9_999)
    return [make_txn_row(
        amount=amount,
        currency=random.choice(["EUR","GBP","AED"]),
        channel=channel,
        status_id=random.choices(
            [approved_sid, declined_sid], weights=[55, 45])[0],
        is_flagged=1,
        is_international=1,
    )]


def pattern_night_owl():
    """Transactions between 01:00 and 04:59 AM — suspicious off-hours activity."""
    rows  = []
    base  = rnd_dt()
    base  = base.replace(hour=random.randint(1, 4),
                         minute=random.randint(0, 59),
                         second=random.randint(0, 59))
    n     = random.randint(2, 8)
    cid   = random.choice(client_ids)
    for i in range(n):
        t = base + timedelta(minutes=i * random.randint(5, 20))
        rows.append(make_txn_row(
            client_id=cid,
            amount=rnd_amount(100, 3_000),
            channel=random.choice(["ONLINE","MOBILE"]),
            is_flagged=1,
            initiated_at=t,
        ))
    return rows


def pattern_international_spree():
    """Rapid cross-border transactions in multiple currencies."""
    rows      = []
    client_id = random.choice(client_ids)
    cards     = client_card_map.get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    start     = rnd_dt()
    foreign   = [c for c in CURRENCIES if c != "USD"]
    n         = random.randint(3, 10)
    for i in range(n):
        t = start + timedelta(minutes=i * random.randint(1, 15))
        rows.append(make_txn_row(
            client_id=client_id, card_id=card_id,
            amount=rnd_amount(200, 4_000),
            currency=random.choice(foreign),
            channel="ONLINE",
            is_flagged=1,
            is_international=1,
            initiated_at=t,
        ))
    return rows


def pattern_account_takeover():
    """New device + new country + high amount — classic ATO signal."""
    return [make_txn_row(
        amount=rnd_amount(500, 5_000),
        channel="ONLINE",
        status_id=random.choices(
            [approved_sid, declined_sid], weights=[65, 35])[0],
        is_flagged=1,
        is_international=random.choice([0, 1]),
        ip_address=fake.ipv4(),
        device_id=fake.md5()[:80],
    )]


FRAUD_PATTERNS = [
    (pattern_velocity_burst,     "velocity burst"),
    (pattern_card_testing,       "card testing"),
    (pattern_high_value_fraud,   "high-value fraud"),
    (pattern_night_owl,          "night-owl"),
    (pattern_international_spree,"international spree"),
    (pattern_account_takeover,   "account takeover"),
]

# ── How many fraud campaigns to inject ──────────────────────────────────────
# Target ~20 % of total transactions as fraudulent.
# Each campaign produces roughly 1-30 rows → we estimate avg=8 rows/campaign.
FRAUD_RATIO          = 0.20
fraud_txn_target     = int(args.transactions * FRAUD_RATIO)
normal_txn_target    = args.transactions - fraud_txn_target
CAMPAIGNS_PER_PATTERN = fraud_txn_target // (len(FRAUD_PATTERNS) * 8)  # rough even split

print(f"   Target: {normal_txn_target:,} normal + ~{fraud_txn_target:,} fraud "
      f"({int(FRAUD_RATIO*100)}% fraud rate)")

# ── Build fraud transaction rows ─────────────────────────────────────────────
fraud_rows = []
for pattern_fn, label in FRAUD_PATTERNS:
    count = 0
    for _ in range(CAMPAIGNS_PER_PATTERN):
        fraud_rows.extend(pattern_fn())
        count += 1
    print(f"      ↳ {label}: {count} campaigns")

print(f"   Generated {len(fraud_rows):,} fraud transaction rows")

# ── Build normal transaction rows ────────────────────────────────────────────
normal_rows = []
for _ in range(normal_txn_target):
    client_id = random.choice(client_ids)
    cards     = client_card_map.get(client_id, [])
    card_id   = random.choice(cards) if cards else None
    pos_id    = random.choice(pos_ids) if random.random() < 0.70 else None
    mid       = pos_merchant.get(pos_id) if pos_id else random.choice(merchant_ids)
    amount    = rnd_amount(1.0, 4000.0)
    currency  = random.choices(CURRENCIES, weights=CUR_WEIGHTS)[0]
    channel   = random.choices(CHANNELS, weights=CHAN_WEIGHTS)[0]
    status_id = random.choice(stat_pool)
    is_intl   = 1 if currency != "USD" and random.random() < 0.3 else 0
    is_recur  = 1 if random.random() < 0.08 else 0
    # Low base flag rate for normal transactions (3%)
    is_flagged = 1 if amount > 2500 and random.random() < 0.03 else 0
    normal_rows.append(make_txn_row(
        client_id=client_id, card_id=card_id,
        pos_id=pos_id, merchant_id=mid,
        amount=amount, currency=currency, channel=channel,
        status_id=status_id,
        is_flagged=is_flagged,
        is_international=is_intl,
        is_recurring=is_recur,
    ))

# ── Shuffle fraud into the normal stream ─────────────────────────────────────
all_txns = normal_rows + fraud_rows
random.shuffle(all_txns)

# ── Batch insert ─────────────────────────────────────────────────────────────
bar      = tqdm(total=len(all_txns), desc="   transactions")
inserted = 0
for chunk in chunks(all_txns, BATCH_SIZE):
    cur.executemany(txn_sql, chunk)
    conn.commit()
    inserted += len(chunk)
    bar.update(len(chunk))
bar.close()
print(f"   ✓ {inserted:,} transactions  "
      f"(fraud rows: {len(fraud_rows):,} / {len(fraud_rows)/inserted*100:.1f}%)")

# ---------------------------------------------------------------------------
# 6. FRAUD ALERTS  — multiple rules per flagged transaction
# ---------------------------------------------------------------------------
print("\n🚨  Inserting fraud alerts …")
cur.execute("SELECT txn_id, amount, channel, is_international "
            "FROM transactions WHERE is_flagged = 1")
flagged_rows = cur.fetchall()

# Rule catalogue — each pattern maps to its most likely triggered rules
RULE_MAP = {
    "high_amount":     ["HIGH_AMOUNT", "VELOCITY_CHECK", "RISK_SCORE_BREACH"],
    "night":           ["NIGHT_TRANSACTION", "UNUSUAL_LOCATION", "OFF_HOURS"],
    "online":          ["NEW_DEVICE", "IP_MISMATCH", "BROWSER_FINGERPRINT"],
    "international":   ["COUNTRY_BLOCK", "CROSS_BORDER", "CURRENCY_MISMATCH"],
    "generic":         ["VELOCITY_CHECK","BIN_MISMATCH","MULTIPLE_DECLINES",
                        "DUPLICATE_TXN","BLACKLIST_HIT","PATTERN_MATCH"],
}

def pick_rules(amount, channel, is_international) -> list:
    """Return 1-3 relevant rules for this transaction."""
    pool = list(RULE_MAP["generic"])
    if amount > 3000:       pool += RULE_MAP["high_amount"]
    if channel in ("ONLINE","MOBILE"): pool += RULE_MAP["online"]
    if is_international:    pool += RULE_MAP["international"]
    n_rules = random.choices([1, 2, 3], weights=[40, 40, 20])[0]
    return random.sample(list(set(pool)), min(n_rules, len(set(pool))))

alert_sql = """
INSERT INTO fraud_alerts
  (txn_id, rule_triggered, risk_score, is_confirmed, reviewed_by, reviewed_at)
VALUES (%s,%s,%s,%s,%s,%s)
"""
alert_rows = []
for txn_id, amount, channel, is_intl in flagged_rows:
    rules = pick_rules(float(amount), channel, is_intl)
    # Higher risk score for larger amounts
    base_risk = 60 if float(amount) < 1000 else 75 if float(amount) < 3000 else 90
    for rule in rules:
        reviewed  = random.random() < 0.70          # 70% of alerts get reviewed
        confirmed = random.choices([1, 0], weights=[40, 60])[0] if reviewed else None
        alert_rows.append((
            txn_id,
            rule,
            min(100, base_risk + random.randint(-5, 10)),
            confirmed,
            fake.name()[:60] if reviewed else None,
            rnd_dt(NOW - timedelta(days=7), NOW) if reviewed else None,
        ))

for chunk in tqdm(list(chunks(alert_rows, BATCH_SIZE)), desc="   fraud alerts"):
    cur.executemany(alert_sql, chunk)
conn.commit()
print(f"   ✓ {len(alert_rows):,} fraud alert records "
      f"({len(flagged_rows):,} flagged transactions, "
      f"avg {len(alert_rows)/max(len(flagged_rows),1):.1f} rules each)")

# ---------------------------------------------------------------------------
# 7. REFUNDS  (~5% of approved)
# ---------------------------------------------------------------------------
print("\n↩️   Inserting refunds …")
cur.execute(f"""
    SELECT txn_id, amount FROM transactions
    WHERE status_id = {approved_sid}
    ORDER BY RAND()
    LIMIT {int(inserted * 0.05)}
""")
refund_candidates = cur.fetchall()

refund_sql = """
INSERT INTO refunds
  (original_txn_id, reference, amount, reason, status_id, initiated_at, resolved_at)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""
refund_reasons = [
    "Customer request","Item not received","Duplicate charge",
    "Fraudulent transaction","Item returned","Service not rendered",
]
refund_rows = []
for txn_id, orig_amount in refund_candidates:
    partial     = random.random() < 0.3
    rfnd_amount = round(float(orig_amount) * random.uniform(0.1, 0.9), 2) \
                  if partial else float(orig_amount)
    init_at     = rnd_dt(NOW - timedelta(days=30), NOW)
    refund_rows.append((
        txn_id, str(uuid.uuid4()), rfnd_amount,
        random.choice(refund_reasons),
        approved_sid,
        init_at,
        init_at + timedelta(hours=random.randint(1, 72)),
    ))

for chunk in chunks(refund_rows, BATCH_SIZE):
    cur.executemany(refund_sql, chunk)
conn.commit()
print(f"   ✓ {len(refund_rows):,} refunds")

# ---------------------------------------------------------------------------
# 8. SETTLEMENT BATCHES
# ---------------------------------------------------------------------------
print("\n🏦  Inserting settlement batches …")
batch_sql = """
INSERT IGNORE INTO settlement_batches
  (merchant_id, batch_date, txn_count, gross_amount, total_fees,
   net_payout, currency, status, settled_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
batch_rows = []
for mid in merchant_ids:
    for days_ago in range(365):
        d      = (NOW - timedelta(days=days_ago)).date()
        gross  = round(random.uniform(500, 50_000), 2)
        fees   = round(gross * random.uniform(0.015, 0.03), 2)
        net    = round(gross - fees, 2)
        status = "SETTLED" if days_ago > 2 else random.choice(["PENDING","PROCESSING"])
        setld  = datetime.combine(d, datetime.min.time()) + timedelta(hours=22) \
                 if status == "SETTLED" else None
        batch_rows.append((mid, d, random.randint(10, 400),
                           gross, fees, net, "USD", status, setld))

for chunk in tqdm(list(chunks(batch_rows, BATCH_SIZE)), desc="   batches"):
    cur.executemany(batch_sql, chunk)
conn.commit()
print(f"   ✓ {len(batch_rows):,} settlement batch records")

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
cur.close()
conn.close()

fraud_pct = len(fraud_rows) / inserted * 100
print("\n" + "="*60)
print("  ✅  DATA GENERATION COMPLETE")
print("="*60)
print(f"  Merchants          : {MERCHANT_COUNT:,}")
print(f"  POS Terminals      : {args.pos:,}")
print(f"  Clients            : {args.clients:,}")
print(f"  Payment Cards      : {len(all_card_ids):,}")
print(f"  Transactions       : {inserted:,}")
print(f"    ├─ Normal        : {len(normal_rows):,}  ({100-fraud_pct:.1f}%)")
print(f"    └─ Fraud         : {len(fraud_rows):,}  ({fraud_pct:.1f}%)")
print(f"  Fraud Alerts       : {len(alert_rows):,}  (avg {len(alert_rows)/max(len(flagged_rows),1):.1f} rules/txn)")
print(f"  Refunds            : {len(refund_rows):,}")
print(f"  Settlement Batches : {len(batch_rows):,}")
print("="*60)
print("\n  Connect via Adminer: http://localhost:8081")
print("  Server: mysql | User: pg_user | DB: payment_gateway")
