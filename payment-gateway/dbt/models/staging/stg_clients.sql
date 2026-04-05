{{ config(materialized='view') }}

SELECT
    client_id,
    first_name,
    last_name,
    CONCAT(first_name, ' ', last_name) AS full_name,
    email,
    phone,
    date_of_birth,
    country_id,
    city,
    risk_score,
    CAST(is_active AS UNSIGNED) AS is_active,
    registered_at
FROM {{ source('payment_gateway', 'clients') }}
