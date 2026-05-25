{{ config(
    materialized='incremental',
    unique_key='product_id',
    incremental_strategy='merge'
) }}

WITH silver AS (
    SELECT * FROM {{ ref('customer_transactions') }}
),

-- deduplicate: if a product_id has multiple names, take the most recent one
ranked AS (
    SELECT
        product_id,
        product_name,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY transaction_date DESC
        ) AS rn
    FROM silver
)

SELECT
    product_id,
    product_name
FROM ranked
WHERE rn = 1