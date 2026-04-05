-- Daily transaction volume, revenue, and fraud metrics
{{ config(materialized='table') }}

SELECT
    DATE(t.initiated_at)                        AS txn_date,
    s.code                                       AS status,
    t.channel,
    t.currency,
    COUNT(*)                                     AS txn_count,
    SUM(t.amount)                                AS gross_amount,
    SUM(t.fee)                                   AS total_fees,
    SUM(t.net_amount)                            AS net_amount,
    SUM(t.is_flagged)                            AS fraud_count,
    ROUND(SUM(t.is_flagged) / COUNT(*) * 100, 2) AS fraud_rate_pct,
    AVG(t.amount)                                AS avg_txn_amount,
    MAX(t.amount)                                AS max_txn_amount
FROM      {{ ref('stg_transactions') }}        t
LEFT JOIN {{ source('payment_gateway', 'transaction_statuses') }} s
       ON t.status_id = s.status_id
GROUP BY  txn_date, s.code, t.channel, t.currency
ORDER BY  txn_date DESC
