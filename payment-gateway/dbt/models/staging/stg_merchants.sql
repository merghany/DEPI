{{ config(materialized='view') }}

SELECT
    merchant_id,
    legal_name,
    trade_name,
    mcc,
    country_id,
    city,
    CAST(is_active AS UNSIGNED) AS is_active,
    onboarded_at
FROM {{ source('payment_gateway', 'merchants') }}
