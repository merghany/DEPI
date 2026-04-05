-- Fraud alert analysis with transaction context
{{ config(materialized='table') }}

SELECT
    fa.alert_id,
    fa.rule_triggered,
    fa.risk_score,
    fa.is_confirmed,
    fa.reviewed_by,
    fa.reviewed_at,
    t.txn_id,
    t.reference,
    t.amount,
    t.currency,
    t.channel,
    t.is_international,
    t.initiated_at,
    cl.full_name                                AS client_name,
    cl.email                                    AS client_email,
    cl.risk_score                               AS client_risk_score,
    m.trade_name                                AS merchant_name,
    CASE
        WHEN fa.is_confirmed = 1 THEN 'Confirmed Fraud'
        WHEN fa.is_confirmed = 0 THEN 'False Positive'
        ELSE 'Pending Review'
    END                                         AS alert_outcome
FROM      {{ ref('stg_fraud_alerts') }}         fa
JOIN      {{ ref('stg_transactions') }}         t  ON fa.txn_id      = t.txn_id
JOIN      {{ ref('stg_clients') }}              cl ON t.client_id    = cl.client_id
LEFT JOIN {{ ref('stg_merchants') }}            m  ON t.merchant_id  = m.merchant_id
ORDER BY  fa.risk_score DESC, t.amount DESC
