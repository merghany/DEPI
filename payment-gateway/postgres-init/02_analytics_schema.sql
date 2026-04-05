-- Connect to analytics DB and create mart tables
\connect analytics

-- ── mart_daily_transaction_summary ───────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_daily_transaction_summary (
    txn_date       DATE,
    status         VARCHAR(30),
    channel        VARCHAR(20),
    currency       CHAR(3),
    txn_count      BIGINT,
    gross_amount   NUMERIC(18,2),
    total_fees     NUMERIC(14,2),
    net_amount     NUMERIC(18,2),
    fraud_count    BIGINT,
    fraud_rate_pct NUMERIC(6,2),
    avg_txn_amount NUMERIC(14,2),
    max_txn_amount NUMERIC(14,2),
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (txn_date, status, channel, currency)
);

-- ── mart_merchant_performance ─────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_merchant_performance (
    merchant_id    INT,
    trade_name     VARCHAR(120),
    legal_name     VARCHAR(120),
    category       VARCHAR(120),
    country        VARCHAR(100),
    total_txns     BIGINT,
    gross_revenue  NUMERIC(18,2),
    total_fees     NUMERIC(14,2),
    net_revenue    NUMERIC(18,2),
    avg_txn_value  NUMERIC(14,2),
    fraud_txns     BIGINT,
    fraud_rate_pct NUMERIC(6,2),
    first_txn_at   TIMESTAMP,
    last_txn_at    TIMESTAMP,
    loaded_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (merchant_id)
);

-- ── mart_client_360 ───────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_client_360 (
    client_id       INT,
    full_name       VARCHAR(130),
    email           VARCHAR(120),
    country_id      SMALLINT,
    risk_score      SMALLINT,
    registered_at   TIMESTAMP,
    card_count      INT,
    total_txns      BIGINT,
    approved_txns   BIGINT,
    declined_txns   BIGINT,
    fraud_txns      BIGINT,
    total_spent     NUMERIC(18,2),
    avg_txn_amount  NUMERIC(14,2),
    max_txn_amount  NUMERIC(14,2),
    last_txn_at     TIMESTAMP,
    loaded_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (client_id)
);

-- ── mart_fraud_analysis ───────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS mart_fraud_analysis (
    alert_id            BIGINT,
    rule_triggered      VARCHAR(80),
    risk_score          SMALLINT,
    is_confirmed        SMALLINT,
    reviewed_by         VARCHAR(60),
    reviewed_at         TIMESTAMP,
    txn_id              BIGINT,
    reference           CHAR(36),
    amount              NUMERIC(14,2),
    currency            CHAR(3),
    channel             VARCHAR(20),
    is_international    SMALLINT,
    initiated_at        TIMESTAMP,
    client_name         VARCHAR(130),
    client_email        VARCHAR(120),
    client_risk_score   SMALLINT,
    merchant_name       VARCHAR(120),
    alert_outcome       VARCHAR(30),
    loaded_at           TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (alert_id)
);

-- Indexes for common query patterns
CREATE INDEX IF NOT EXISTS idx_daily_summary_date     ON mart_daily_transaction_summary (txn_date);
CREATE INDEX IF NOT EXISTS idx_merchant_revenue        ON mart_merchant_performance (gross_revenue DESC);
CREATE INDEX IF NOT EXISTS idx_client_risk             ON mart_client_360 (risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_client_spent            ON mart_client_360 (total_spent DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_risk_score        ON mart_fraud_analysis (risk_score DESC);
CREATE INDEX IF NOT EXISTS idx_fraud_outcome           ON mart_fraud_analysis (alert_outcome);
CREATE INDEX IF NOT EXISTS idx_fraud_initiated         ON mart_fraud_analysis (initiated_at);

-- Grant dbt_user access to all analytics tables
GRANT USAGE ON SCHEMA public TO dbt_user;
GRANT SELECT, INSERT, UPDATE, TRUNCATE ON ALL TABLES IN SCHEMA public TO dbt_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT SELECT, INSERT, UPDATE, TRUNCATE ON TABLES TO dbt_user;
