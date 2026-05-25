{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

WITH silver AS (
    SELECT * FROM {{ ref('customer_transactions') }}
)

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    quantity,
    price,
    tax,
    ROUND((price * quantity) + tax, 2) AS total_amount
FROM silver