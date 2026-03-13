#!/usr/bin/env python3
"""
Electronic Payment Gateway — Sample Data Generator
====================================================
Generates:  10 000 clients | 1 000 POS terminals | 150 000+ transactions
Period   :  last 365 days back from today

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
from datetime import datetime, timedelta, date
from decimal import Decimal

import mysql.connector
from faker import Faker
from tqdm import tqdm

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Payment Gateway Data Generator")
parser.add_argument("--host",     default="localhost")
parser.add_argument("--port",     type=int, default=3306)
parser.add_argument("--user",     default="pg_user")
parser.add_argument("--password", default="pg_password_2024")
parser.add_argument("--database", default="payment_gateway")
parser.add_argument("--clients",      type=int, default=10_000)
parser.add_argument("--pos",          type=int, default=1_000)
parser.add_argument("--transactions", type=int, default=155_000)
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
# Helpers
# ---------------------------------------------------------------------------
NOW          = datetime.now()
YEAR_AGO     = NOW - timedelta(days=365)
BATCH_SIZE   = 2_000

def rnd_dt(start: datetime = YEAR_AGO, end: datetime = NOW) -> datetime:
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def rnd_amount(low=1.0, high=5000.0) -> float:
    # Log-normal distribution → most txns are small, rare large ones
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
status_rows   = cur.fetchall()
status_ids    = [r[0] for r in status_rows]
approved_sid  = next(r[0] for r in status_rows if r[1] == "APPROVED")
declined_sid  = next(r[0] for r in status_rows if r[1] == "DECLINED")
pending_sid   = next(r[0] for r in status_rows if r[1] == "PENDING")
failed_sid    = next(r[0] for r in status_rows if r[1] == "FAILED")
settled_sid   = next(r[0] for r in status_rows if r[1] == "SETTLED")

cur.execute("SELECT network_id FROM card_networks")
network_ids = [r[0] for r in cur.fetchall()]

cur.execute("SELECT mcc FROM merchant_categories")
mcc_list = [r[0] for r in cur.fetchall()]

# ---------------------------------------------------------------------------
# 1. MERCHANTS  (200 merchants backing 1 000 POS)
# ---------------------------------------------------------------------------
print("\n📦  Inserting merchants …")
MERCHANT_COUNT = 200
merchant_sql = """
INSERT INTO merchants
  (legal_name, trade_name, mcc, tax_id, country_id, city, address, zip_code, email, phone, is_active, onboarded_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
merchant_rows = []
for _ in range(MERCHANT_COUNT):
    company = fake.company()
    mcc     = random.choice(mcc_list)
    onboard = rnd_dt(YEAR_AGO - timedelta(days=365), YEAR_AGO)
    merchant_rows.append((
        company,
        company + " " + fake.company_suffix(),
        mcc,
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
# 2. POS TERMINALS  (1 000)
# ---------------------------------------------------------------------------
print("🖥️   Inserting POS terminals …")
pos_sql = """
INSERT INTO pos_terminals
  (serial_number, merchant_id, model, firmware_version, location_label, is_active, installed_at, last_seen_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
"""
models    = ["Verifone VX520","Ingenico iCT250","PAX S300","Newland N910","Sunmi P2","Castles VEGA3000"]
firmwares = ["v2.1.0","v2.3.5","v3.0.1","v3.2.0","v4.0.0"]
labels    = ["Cashier 1","Cashier 2","Counter","Drive-Thru","Self-Service","Mobile Unit","Entrance","Exit"]

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
pos_rows_db = cur.fetchall()
pos_ids      = [r[0] for r in pos_rows_db]
pos_merchant = {r[0]: r[1] for r in pos_rows_db}
print(f"   ✓ {len(pos_ids)} POS terminals")

# ---------------------------------------------------------------------------
# 3. CLIENTS  (10 000)
# ---------------------------------------------------------------------------
print("👤  Inserting clients …")
client_sql = """
INSERT INTO clients
  (first_name, last_name, email, phone, date_of_birth, national_id,
   country_id, city, address, zip_code, risk_score, is_active, registered_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
client_rows = []
emails_used = set()
for _ in range(args.clients):
    email = fake.unique.email()
    reg   = rnd_dt(YEAR_AGO - timedelta(days=730), YEAR_AGO)
    dob   = fake.date_of_birth(minimum_age=18, maximum_age=75)
    client_rows.append((
        fake.first_name(),
        fake.last_name(),
        email,
        fake.phone_number()[:30],
        dob,
        fake.bothify("##########"),
        random.choice(country_ids),
        fake.city(),
        fake.street_address(),
        fake.postcode(),
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
# 4. PAYMENT CARDS  (1–4 cards per client)
# ---------------------------------------------------------------------------
print("💳  Inserting payment cards …")
card_sql = """
INSERT INTO payment_cards
  (client_id, network_id, masked_pan, card_token, holder_name,
   expiry_month, expiry_year, is_default, is_active, issued_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
networks_prefix = {1: "4", 2: "51", 3: "37", 4: "60"}
card_rows       = []
client_cards    = {}   # client_id -> [card_id placeholders]

for cid in client_ids:
    n_cards = random.choices([1,2,3,4], weights=[50,30,15,5])[0]
    cards   = []
    for k in range(n_cards):
        net  = random.choice(network_ids)
        pan  = networks_prefix.get(net, "4") + fake.numerify("## **** **** ####")
        exp_y = random.randint(NOW.year, NOW.year + 5)
        exp_m = random.randint(1, 12)
        cards.append((
            cid, net, pan[:19], str(uuid.uuid4()),
            fake.name()[:100],
            exp_m, exp_y,
            1 if k == 0 else 0,
            1,
            rnd_dt(YEAR_AGO - timedelta(days=365), YEAR_AGO),
        ))
    card_rows.extend(cards)
    client_cards[cid] = len(cards)

for chunk in tqdm(list(chunks(card_rows, BATCH_SIZE)), desc="   cards"):
    cur.executemany(card_sql, chunk)
conn.commit()

cur.execute("SELECT card_id, client_id FROM payment_cards")
card_rows_db     = cur.fetchall()
client_card_map  = {}
for card_id, cid in card_rows_db:
    client_card_map.setdefault(cid, []).append(card_id)
all_card_ids = [r[0] for r in card_rows_db]
print(f"   ✓ {len(all_card_ids)} payment cards")

# ---------------------------------------------------------------------------
# 5. TRANSACTIONS  (≥ 150 000)
# ---------------------------------------------------------------------------
print("💰  Inserting transactions …")

channels     = ["POS","ONLINE","ATM","MOBILE","IVR"]
chan_weights  = [55, 25, 8, 10, 2]
currencies   = ["USD","EUR","GBP","EGP","SAR","AED","CAD","AUD"]
cur_weights  = [50,  15,  10,  8,   6,   5,   4,   2]

# Status distribution: mostly approved
stat_pool    = (
    [approved_sid] * 75 +
    [declined_sid] * 12 +
    [settled_sid]  * 10 +
    [pending_sid]  *  2 +
    [failed_sid]   *  1
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

txn_rows      = []
flagged_txns  = []   # collect for fraud_alerts
purchase_tid  = 1    # type_id for PURCHASE

total_batches = math.ceil(args.transactions / BATCH_SIZE)
bar = tqdm(total=args.transactions, desc="   transactions")

inserted = 0
while inserted < args.transactions:
    chunk_data = []
    chunk_size = min(BATCH_SIZE, args.transactions - inserted)

    for _ in range(chunk_size):
        client_id  = random.choice(client_ids)
        cards      = client_card_map.get(client_id, [])
        card_id    = random.choice(cards) if cards else None
        pos_id     = random.choice(pos_ids) if random.random() < 0.70 else None
        merchant_id= pos_merchant.get(pos_id) if pos_id else random.choice(merchant_ids)
        type_id    = random.choice(type_ids)
        status_id  = random.choice(stat_pool)
        amount     = rnd_amount(1.0, 4000.0)
        fee        = calc_fee(amount)
        currency   = random.choices(currencies, weights=cur_weights)[0]
        channel    = random.choices(channels, weights=chan_weights)[0]
        is_intl    = 1 if currency != "USD" and random.random() < 0.3 else 0
        is_recur   = 1 if random.random() < 0.08 else 0
        is_flagged = 1 if amount > 2500 and random.random() < 0.05 else 0
        fail_rsn   = None
        if status_id == declined_sid:
            fail_rsn = random.choice([
                "Insufficient funds","Card expired","Invalid CVV",
                "Blocked card","Limit exceeded","Suspected fraud",
            ])
        init_at    = rnd_dt()
        proc_at    = init_at + timedelta(seconds=random.randint(1, 8))
        settled    = (init_at + timedelta(days=random.randint(1,3))).date() \
                     if status_id == settled_sid else None

        chunk_data.append((
            str(uuid.uuid4()), client_id, card_id, pos_id, merchant_id,
            type_id, status_id, amount, currency, fee,
            fake.bothify("??????")[:20] if status_id == approved_sid else None,
            channel,
            fake.ipv4() if channel in ("ONLINE","MOBILE") else None,
            fake.md5()[:80] if channel in ("ONLINE","MOBILE") else None,
            is_recur, is_intl, is_flagged,
            fail_rsn, init_at, proc_at, settled,
        ))

    cur.executemany(txn_sql, chunk_data)
    conn.commit()
    inserted += len(chunk_data)
    bar.update(len(chunk_data))

bar.close()
print(f"   ✓ {inserted} transactions")

# ---------------------------------------------------------------------------
# 6. FRAUD ALERTS  (for flagged transactions)
# ---------------------------------------------------------------------------
print("🚨  Inserting fraud alerts …")
cur.execute("SELECT txn_id FROM transactions WHERE is_flagged = 1")
flagged = [r[0] for r in cur.fetchall()]

rules = [
    "VELOCITY_CHECK","HIGH_AMOUNT","UNUSUAL_LOCATION",
    "BIN_MISMATCH","MULTIPLE_DECLINES","NIGHT_TRANSACTION",
    "NEW_DEVICE","COUNTRY_BLOCK",
]
alert_sql = """
INSERT INTO fraud_alerts (txn_id, rule_triggered, risk_score, is_confirmed, reviewed_by, reviewed_at)
VALUES (%s,%s,%s,%s,%s,%s)
"""
alert_rows = []
for tid in flagged:
    reviewed   = random.random() < 0.60
    confirmed  = random.choices([1, 0], weights=[20, 80])[0] if reviewed else None
    reviewer   = fake.name()[:60] if reviewed else None
    rev_at     = rnd_dt(NOW - timedelta(days=3), NOW) if reviewed else None
    alert_rows.append((
        tid,
        random.choice(rules),
        random.randint(50, 100),
        confirmed, reviewer, rev_at,
    ))

for chunk in chunks(alert_rows, BATCH_SIZE):
    cur.executemany(alert_sql, chunk)
conn.commit()
print(f"   ✓ {len(alert_rows)} fraud alerts")

# ---------------------------------------------------------------------------
# 7. REFUNDS  (≈5% of approved transactions)
# ---------------------------------------------------------------------------
print("↩️   Inserting refunds …")
cur.execute(f"""
    SELECT txn_id, amount FROM transactions
    WHERE status_id = {approved_sid}
    ORDER BY RAND() LIMIT {int(inserted * 0.05)}
""")
refund_candidates = cur.fetchall()

refund_sql = """
INSERT INTO refunds (original_txn_id, reference, amount, reason, status_id, initiated_at, resolved_at)
VALUES (%s,%s,%s,%s,%s,%s,%s)
"""
refund_reasons = [
    "Customer request","Item not received","Duplicate charge",
    "Fraudulent transaction","Item returned","Service not rendered",
]
refund_rows = []
for txn_id, orig_amount in refund_candidates:
    partial     = random.random() < 0.3
    rfnd_amount = round(orig_amount * random.uniform(0.1, 0.9), 2) if partial else orig_amount
    init_at     = rnd_dt(NOW - timedelta(days=30), NOW)
    resolved    = init_at + timedelta(hours=random.randint(1, 72))
    refund_rows.append((
        txn_id, str(uuid.uuid4()), rfnd_amount,
        random.choice(refund_reasons),
        approved_sid, init_at, resolved,
    ))

for chunk in chunks(refund_rows, BATCH_SIZE):
    cur.executemany(refund_sql, chunk)
conn.commit()
print(f"   ✓ {len(refund_rows)} refunds")

# ---------------------------------------------------------------------------
# 8. SETTLEMENT BATCHES  (daily per merchant, last 365 days)
# ---------------------------------------------------------------------------
print("🏦  Inserting settlement batches …")
batch_sql = """
INSERT IGNORE INTO settlement_batches
  (merchant_id, batch_date, txn_count, gross_amount, total_fees, net_payout, currency, status, settled_at)
VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
"""
batch_rows = []
for mid in merchant_ids:
    for days_ago in range(365):
        d = (NOW - timedelta(days=days_ago)).date()
        gross  = round(random.uniform(500, 50000), 2)
        fees   = round(gross * random.uniform(0.015, 0.03), 2)
        net    = round(gross - fees, 2)
        count  = random.randint(10, 400)
        status = "SETTLED" if days_ago > 2 else random.choice(["PENDING","PROCESSING"])
        setld  = datetime.combine(d, datetime.min.time()) + timedelta(hours=22) \
                 if status == "SETTLED" else None
        batch_rows.append((mid, d, count, gross, fees, net, "USD", status, setld))

for chunk in tqdm(list(chunks(batch_rows, BATCH_SIZE)), desc="   batches"):
    cur.executemany(batch_sql, chunk)
conn.commit()
print(f"   ✓ {len(batch_rows)} settlement batch records")

# ---------------------------------------------------------------------------
# Final summary
# ---------------------------------------------------------------------------
cur.close()
conn.close()

print("\n" + "="*55)
print("  ✅  DATA GENERATION COMPLETE")
print("="*55)
print(f"  Merchants          : {MERCHANT_COUNT}")
print(f"  POS Terminals      : {args.pos}")
print(f"  Clients            : {args.clients}")
print(f"  Payment Cards      : {len(all_card_ids)}")
print(f"  Transactions       : {inserted}")
print(f"  Fraud Alerts       : {len(alert_rows)}")
print(f"  Refunds            : {len(refund_rows)}")
print(f"  Settlement Batches : {len(batch_rows)}")
print("="*55)
print("\n  Connect via Adminer: http://localhost:8080")
print("  Server: mysql | User: pg_user | DB: payment_gateway")
