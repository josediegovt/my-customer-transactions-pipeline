WITH stg AS (
    SELECT * FROM {{ ref('stg_customer_transactions') }}
)

SELECT
  REGEXP_REPLACE(transaction_id, '^[[:alpha:]]+', '')::integer AS transaction_id,
  customer_id::NUMERIC::INTEGER AS customer_id,
  CASE
    WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$'
      THEN TO_DATE(transaction_date, 'YYYY-MM-DD')
    WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$'
      THEN TO_DATE(transaction_date, 'DD-MM-YYYY')
  END AS transaction_date,
  REGEXP_REPLACE(product_id, '^[[:alpha:]]+', '')::integer AS product_id,
  product_name,
  quantity::NUMERIC::INTEGER AS quantity,
  price::NUMERIC(10, 2) AS price,
  tax::NUMERIC(10, 2) AS tax
FROM 
  stg
WHERE
  failure_reasons IS NULL