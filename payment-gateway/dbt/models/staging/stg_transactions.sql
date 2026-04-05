-- Staging: clean and type-cast raw transactions
{{ config(materialized='view') }}

SELECT
    txn_id,
    reference,
    client_id,
    card_id,
    pos_id,
    merchant_id,
    type_id,
    status_id,
    CAST(amount     AS DECIMAL(14,2))  AS amount,
    currency,
    CAST(fee        AS DECIMAL(10,2))  AS fee,
    CAST(net_amount AS DECIMAL(14,2))  AS net_amount,
    auth_code,
    channel,
    ip_address,
    device_id,
    CAST(is_recurring    AS UNSIGNED) AS is_recurring,
    CAST(is_international AS UNSIGNED) AS is_international,
    CAST(is_flagged      AS UNSIGNED) AS is_flagged,
    failure_reason,
    initiated_at,
    processed_at,
    settled_at,
    created_at
FROM {{ source('payment_gateway', 'transactions') }}
