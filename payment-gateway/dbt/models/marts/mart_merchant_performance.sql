-- Merchant-level revenue, transaction volume, and fraud summary
{{ config(materialized='table') }}

WITH approved AS (
    SELECT *
    FROM {{ ref('stg_transactions') }} t
    JOIN {{ source('payment_gateway', 'transaction_statuses') }} s
      ON t.status_id = s.status_id
    WHERE s.code = 'APPROVED'
)
SELECT
    m.merchant_id,
    m.trade_name,
    m.legal_name,
    mc.description                               AS category,
    c.name                                       AS country,
    COUNT(a.txn_id)                              AS total_txns,
    SUM(a.amount)                                AS gross_revenue,
    SUM(a.fee)                                   AS total_fees,
    SUM(a.net_amount)                            AS net_revenue,
    AVG(a.amount)                                AS avg_txn_value,
    SUM(a.is_flagged)                            AS fraud_txns,
    ROUND(SUM(a.is_flagged)/COUNT(*)*100, 2)     AS fraud_rate_pct,
    MIN(a.initiated_at)                          AS first_txn_at,
    MAX(a.initiated_at)                          AS last_txn_at
FROM      {{ ref('stg_merchants') }}            m
LEFT JOIN approved                              a  ON m.merchant_id  = a.merchant_id
LEFT JOIN {{ source('payment_gateway', 'merchant_categories') }} mc ON m.mcc = mc.mcc
LEFT JOIN {{ source('payment_gateway', 'countries') }}           c  ON m.country_id = c.country_id
GROUP BY  m.merchant_id, m.trade_name, m.legal_name, mc.description, c.name
ORDER BY  gross_revenue DESC
