{{ config(materialized='view') }}

SELECT
    alert_id,
    txn_id,
    rule_triggered,
    risk_score,
    is_confirmed,
    reviewed_by,
    reviewed_at,
    created_at
FROM {{ source('payment_gateway', 'fraud_alerts') }}
