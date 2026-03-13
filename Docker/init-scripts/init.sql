-- ============================================================
-- Electronic Payment Gateway Database Schema
-- ============================================================

CREATE DATABASE IF NOT EXISTS payment_gateway
  CHARACTER SET utf8mb4
  COLLATE utf8mb4_unicode_ci;

USE payment_gateway;

-- ============================================================
-- LOOKUP / REFERENCE TABLES
-- ============================================================

CREATE TABLE IF NOT EXISTS countries (
    country_id   SMALLINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code         CHAR(2)      NOT NULL UNIQUE,
    name         VARCHAR(100) NOT NULL,
    currency     CHAR(3)      NOT NULL,
    created_at   TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS transaction_types (
    type_id      TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code         VARCHAR(30)  NOT NULL UNIQUE,
    label        VARCHAR(80)  NOT NULL,
    is_debit     TINYINT(1)   NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS transaction_statuses (
    status_id    TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    code         VARCHAR(30)  NOT NULL UNIQUE,
    label        VARCHAR(80)  NOT NULL
);

CREATE TABLE IF NOT EXISTS card_networks (
    network_id   TINYINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    name         VARCHAR(40)  NOT NULL UNIQUE,
    prefix_regex VARCHAR(60)  NOT NULL
);

CREATE TABLE IF NOT EXISTS merchant_categories (
    mcc          SMALLINT UNSIGNED PRIMARY KEY,
    description  VARCHAR(120) NOT NULL
);

-- ============================================================
-- MERCHANTS / BUSINESSES
-- ============================================================

CREATE TABLE IF NOT EXISTS merchants (
    merchant_id      INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    legal_name       VARCHAR(120) NOT NULL,
    trade_name       VARCHAR(120),
    mcc              SMALLINT UNSIGNED,
    tax_id           VARCHAR(30)  UNIQUE,
    country_id       SMALLINT UNSIGNED,
    city             VARCHAR(80),
    address          VARCHAR(200),
    zip_code         VARCHAR(20),
    email            VARCHAR(120),
    phone            VARCHAR(30),
    is_active        TINYINT(1)   NOT NULL DEFAULT 1,
    onboarded_at     DATETIME     NOT NULL,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_merchant_country  FOREIGN KEY (country_id)  REFERENCES countries (country_id),
    CONSTRAINT fk_merchant_mcc      FOREIGN KEY (mcc)         REFERENCES merchant_categories (mcc)
);

-- ============================================================
-- POINT OF SALE TERMINALS (1 000)
-- ============================================================

CREATE TABLE IF NOT EXISTS pos_terminals (
    pos_id           INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    serial_number    VARCHAR(50)  NOT NULL UNIQUE,
    merchant_id      INT UNSIGNED NOT NULL,
    model            VARCHAR(60),
    firmware_version VARCHAR(20),
    location_label   VARCHAR(100),
    is_active        TINYINT(1)   NOT NULL DEFAULT 1,
    installed_at     DATETIME     NOT NULL,
    last_seen_at     DATETIME,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_pos_merchant FOREIGN KEY (merchant_id) REFERENCES merchants (merchant_id)
);

-- ============================================================
-- CLIENTS (10 000)
-- ============================================================

CREATE TABLE IF NOT EXISTS clients (
    client_id        INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    first_name       VARCHAR(60)  NOT NULL,
    last_name        VARCHAR(60)  NOT NULL,
    email            VARCHAR(120) NOT NULL UNIQUE,
    phone            VARCHAR(30),
    date_of_birth    DATE,
    national_id      VARCHAR(30)  UNIQUE,
    country_id       SMALLINT UNSIGNED,
    city             VARCHAR(80),
    address          VARCHAR(200),
    zip_code         VARCHAR(20),
    risk_score       TINYINT UNSIGNED DEFAULT 10,
    is_active        TINYINT(1)   NOT NULL DEFAULT 1,
    registered_at    DATETIME     NOT NULL,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_client_country FOREIGN KEY (country_id) REFERENCES countries (country_id)
);

-- ============================================================
-- PAYMENT CARDS / INSTRUMENTS
-- ============================================================

CREATE TABLE IF NOT EXISTS payment_cards (
    card_id          INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    client_id        INT UNSIGNED NOT NULL,
    network_id       TINYINT UNSIGNED NOT NULL,
    masked_pan       VARCHAR(19)  NOT NULL,  -- e.g. 4111 **** **** 1111
    card_token       CHAR(36)     NOT NULL UNIQUE,
    holder_name      VARCHAR(100) NOT NULL,
    expiry_month     TINYINT UNSIGNED NOT NULL,
    expiry_year      SMALLINT UNSIGNED NOT NULL,
    is_default       TINYINT(1)   NOT NULL DEFAULT 0,
    is_active        TINYINT(1)   NOT NULL DEFAULT 1,
    issued_at        DATETIME     NOT NULL,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_card_client  FOREIGN KEY (client_id)  REFERENCES clients (client_id),
    CONSTRAINT fk_card_network FOREIGN KEY (network_id) REFERENCES card_networks (network_id)
);

-- ============================================================
-- TRANSACTIONS (≥ 150 000)
-- ============================================================

CREATE TABLE IF NOT EXISTS transactions (
    txn_id           BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    reference        CHAR(36)     NOT NULL UNIQUE,   -- UUID
    client_id        INT UNSIGNED NOT NULL,
    card_id          INT UNSIGNED,
    pos_id           INT UNSIGNED,
    merchant_id      INT UNSIGNED,
    type_id          TINYINT UNSIGNED NOT NULL,
    status_id        TINYINT UNSIGNED NOT NULL,
    amount           DECIMAL(14,2) NOT NULL,
    currency         CHAR(3)       NOT NULL DEFAULT 'USD',
    fee              DECIMAL(10,2) NOT NULL DEFAULT 0.00,
    net_amount       DECIMAL(14,2) GENERATED ALWAYS AS (amount - fee) STORED,
    auth_code        VARCHAR(20),
    channel          ENUM('POS','ONLINE','ATM','MOBILE','IVR') NOT NULL DEFAULT 'POS',
    ip_address       VARCHAR(45),
    device_id        VARCHAR(80),
    is_recurring     TINYINT(1)   NOT NULL DEFAULT 0,
    is_international TINYINT(1)   NOT NULL DEFAULT 0,
    is_flagged       TINYINT(1)   NOT NULL DEFAULT 0,
    failure_reason   VARCHAR(120),
    initiated_at     DATETIME     NOT NULL,
    processed_at     DATETIME,
    settled_at       DATE,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_txn_client   FOREIGN KEY (client_id)  REFERENCES clients (client_id),
    CONSTRAINT fk_txn_card     FOREIGN KEY (card_id)    REFERENCES payment_cards (card_id),
    CONSTRAINT fk_txn_pos      FOREIGN KEY (pos_id)     REFERENCES pos_terminals (pos_id),
    CONSTRAINT fk_txn_merchant FOREIGN KEY (merchant_id) REFERENCES merchants (merchant_id),
    CONSTRAINT fk_txn_type     FOREIGN KEY (type_id)    REFERENCES transaction_types (type_id),
    CONSTRAINT fk_txn_status   FOREIGN KEY (status_id)  REFERENCES transaction_statuses (status_id)
);

-- ============================================================
-- REFUNDS / CHARGEBACKS
-- ============================================================

CREATE TABLE IF NOT EXISTS refunds (
    refund_id        BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    original_txn_id  BIGINT UNSIGNED NOT NULL,
    reference        CHAR(36)     NOT NULL UNIQUE,
    amount           DECIMAL(14,2) NOT NULL,
    reason           VARCHAR(200),
    status_id        TINYINT UNSIGNED NOT NULL,
    initiated_at     DATETIME     NOT NULL,
    resolved_at      DATETIME,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_refund_txn    FOREIGN KEY (original_txn_id) REFERENCES transactions (txn_id),
    CONSTRAINT fk_refund_status FOREIGN KEY (status_id)       REFERENCES transaction_statuses (status_id)
);

-- ============================================================
-- FRAUD ALERTS
-- ============================================================

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id         BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    txn_id           BIGINT UNSIGNED NOT NULL,
    rule_triggered   VARCHAR(80)  NOT NULL,
    risk_score       TINYINT UNSIGNED NOT NULL,
    is_confirmed     TINYINT(1)   DEFAULT NULL,
    reviewed_by      VARCHAR(60),
    reviewed_at      DATETIME,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_alert_txn FOREIGN KEY (txn_id) REFERENCES transactions (txn_id)
);

-- ============================================================
-- SETTLEMENT BATCHES
-- ============================================================

CREATE TABLE IF NOT EXISTS settlement_batches (
    batch_id         INT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
    merchant_id      INT UNSIGNED NOT NULL,
    batch_date       DATE         NOT NULL,
    txn_count        INT UNSIGNED NOT NULL DEFAULT 0,
    gross_amount     DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    total_fees       DECIMAL(14,2) NOT NULL DEFAULT 0.00,
    net_payout       DECIMAL(16,2) NOT NULL DEFAULT 0.00,
    currency         CHAR(3)       NOT NULL DEFAULT 'USD',
    status           ENUM('PENDING','PROCESSING','SETTLED','FAILED') DEFAULT 'PENDING',
    settled_at       DATETIME,
    created_at       TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_batch_merchant FOREIGN KEY (merchant_id) REFERENCES merchants (merchant_id),
    UNIQUE KEY uq_batch (merchant_id, batch_date)
);

-- ============================================================
-- INDEXES
-- ============================================================

CREATE INDEX idx_txn_client       ON transactions (client_id);
CREATE INDEX idx_txn_card         ON transactions (card_id);
CREATE INDEX idx_txn_pos          ON transactions (pos_id);
CREATE INDEX idx_txn_merchant     ON transactions (merchant_id);
CREATE INDEX idx_txn_initiated    ON transactions (initiated_at);
CREATE INDEX idx_txn_status       ON transactions (status_id);
CREATE INDEX idx_txn_amount       ON transactions (amount);
CREATE INDEX idx_txn_flagged      ON transactions (is_flagged);
CREATE INDEX idx_txn_channel      ON transactions (channel);
CREATE INDEX idx_card_client      ON payment_cards (client_id);
CREATE INDEX idx_pos_merchant     ON pos_terminals (merchant_id);
CREATE INDEX idx_fraud_txn        ON fraud_alerts (txn_id);
CREATE INDEX idx_refund_txn       ON refunds (original_txn_id);
CREATE INDEX idx_settlement_date  ON settlement_batches (batch_date);

-- ============================================================
-- SEED REFERENCE DATA
-- ============================================================

INSERT INTO countries (code, name, currency) VALUES
  ('US','United States','USD'),('GB','United Kingdom','GBP'),
  ('DE','Germany','EUR'),('FR','France','EUR'),
  ('EG','Egypt','EGP'),('SA','Saudi Arabia','SAR'),
  ('AE','UAE','AED'),('CA','Canada','CAD'),
  ('AU','Australia','AUD'),('NG','Nigeria','NGN');

INSERT INTO transaction_types (code, label, is_debit) VALUES
  ('PURCHASE',   'Card Purchase',        1),
  ('REFUND',     'Refund',               0),
  ('WITHDRAWAL', 'ATM Withdrawal',       1),
  ('TRANSFER',   'P2P Transfer',         1),
  ('TOP_UP',     'Wallet Top-Up',        0),
  ('PAYMENT',    'Bill Payment',         1),
  ('REVERSAL',   'Transaction Reversal', 0);

INSERT INTO transaction_statuses (code, label) VALUES
  ('APPROVED',   'Approved'),
  ('DECLINED',   'Declined'),
  ('PENDING',    'Pending'),
  ('REVERSED',   'Reversed'),
  ('FAILED',     'Failed'),
  ('SETTLED',    'Settled'),
  ('CHARGEBACK', 'Chargeback');

INSERT INTO card_networks (name, prefix_regex) VALUES
  ('Visa',       '^4'),
  ('Mastercard', '^5[1-5]'),
  ('Amex',       '^3[47]'),
  ('Discover',   '^6(?:011|5)');

INSERT INTO merchant_categories (mcc, description) VALUES
  (5411,'Grocery Stores'),(5812,'Restaurants'),(5541,'Gas Stations'),
  (5912,'Drug Stores'),(5732,'Electronics Stores'),(5651,'Clothing Stores'),
  (7011,'Hotels'),(4111,'Transportation'),(5999,'Misc Retail'),
  (5045,'Computers & Peripherals'),(5400,'Supermarkets'),(7841,'Video Rental'),
  (5621,'Women Clothing'),(5311,'Department Stores'),(5691,'Men Clothing');
