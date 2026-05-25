WITH 
  stg AS (
    SELECT 
      * 
    FROM 
      {{ ref('stg_customer_transactions') }}
)
 
SELECT
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    product_name,
    quantity,
    price,
    tax,
    failure_reasons
FROM 
  stg
WHERE 
  failure_reasons IS NOT NULL