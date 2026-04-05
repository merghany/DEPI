-- Client-level 360 view: spend, risk, fraud, and card usage
{{ config(materialized='table') }}

WITH txn_stats AS (
    SELECT
        t.client_id,
        COUNT(*)                                 AS total_txns,
        SUM(CASE WHEN s.code='APPROVED'  THEN 1 ELSE 0 END) AS approved_txns,
        SUM(CASE WHEN s.code='DECLINED'  THEN 1 ELSE 0 END) AS declined_txns,
        SUM(CASE WHEN t.is_flagged=1     THEN 1 ELSE 0 END) AS fraud_txns,
        SUM(t.amount)                            AS total_spent,
        AVG(t.amount)                            AS avg_txn_amount,
        MAX(t.amount)                            AS max_txn_amount,
        MAX(t.initiated_at)                      AS last_txn_at
    FROM {{ ref('stg_transactions') }}           t
    JOIN {{ source('payment_gateway','transaction_statuses') }} s
      ON t.status_id = s.status_id
    GROUP BY t.client_id
),
card_stats AS (
    SELECT client_id, COUNT(*) AS card_count
    FROM {{ source('payment_gateway','payment_cards') }}
    GROUP BY client_id
)
SELECT
    cl.client_id,
    cl.full_name,
    cl.email,
    cl.country_id,
    cl.risk_score,
    cl.registered_at,
    COALESCE(cs.card_count,     0)              AS card_count,
    COALESCE(ts.total_txns,     0)              AS total_txns,
    COALESCE(ts.approved_txns,  0)              AS approved_txns,
    COALESCE(ts.declined_txns,  0)              AS declined_txns,
    COALESCE(ts.fraud_txns,     0)              AS fraud_txns,
    COALESCE(ts.total_spent,    0)              AS total_spent,
    COALESCE(ts.avg_txn_amount, 0)              AS avg_txn_amount,
    COALESCE(ts.max_txn_amount, 0)              AS max_txn_amount,
    ts.last_txn_at
FROM      {{ ref('stg_clients') }}              cl
LEFT JOIN txn_stats                             ts ON cl.client_id = ts.client_id
LEFT JOIN card_stats                            cs ON cl.client_id = cs.client_id
