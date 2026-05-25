{{ config(materialized='ephemeral') }}

WITH 
  source AS (
    SELECT 
      * 
    FROM 
      {{ source('bronze', 'customer_transactions') }}
),
  validated AS (
    SELECT
        transaction_id,
        customer_id,
        transaction_date,
        product_id,
        product_name,
        quantity,
        price,
        tax,

        -- collect all failure reasons into an array, filtering out NULLs
        NULLIF(ARRAY_REMOVE(ARRAY[
            CASE
              WHEN transaction_id IS NULL OR transaction_id = ''
                THEN 'transaction_id is null or empty'
              WHEN REGEXP_REPLACE(transaction_id, '^[[:alpha:]]+', '') !~ '^-?\d+$'
                THEN 'transaction_id is not a valid integer: ' || transaction_id
            END,
            CASE
                WHEN customer_id IS NULL OR customer_id = ''
                    THEN 'customer_id is null or empty'
            END,
            CASE
                WHEN transaction_date IS NULL OR transaction_date = ''
                    THEN 'transaction_date is null or empty'
                WHEN transaction_date ~ '^\d{4}-\d{2}-\d{2}$'
                    AND {{ safe_to_date('transaction_date', 'YYYY-MM-DD') }} IS NULL
                    THEN 'transaction_date is not a valid date: ' || transaction_date
                WHEN transaction_date ~ '^\d{2}-\d{2}-\d{4}$'
                    AND {{ safe_to_date('transaction_date', 'DD-MM-YYYY') }} IS NULL
                    THEN 'transaction_date is not a valid date: ' || transaction_date
                WHEN transaction_date !~ '^\d{4}-\d{2}-\d{2}$'
                    AND transaction_date !~ '^\d{2}-\d{2}-\d{4}$'
                    THEN 'transaction_date has unrecognised format: ' || transaction_date
            END,
            CASE
              WHEN product_id IS NULL OR product_id = ''
                THEN 'product_id is null or empty'
              WHEN REGEXP_REPLACE(product_id, '^[[:alpha:]]+', '') !~ '^-?\d+$'
                THEN 'product_id is not a valid integer: ' || product_id
            END,
            CASE
                WHEN quantity IS NULL OR quantity = ''
                    THEN 'quantity is null or empty'
                WHEN quantity !~ '^-?\d+(\.0+)?$'
                    THEN 'quantity is not a valid integer: ' || quantity
                WHEN quantity::numeric <= 0
                    THEN 'quantity is not positive: ' || quantity
            END,
            CASE
                WHEN price IS NULL OR price = ''
                    THEN 'price is null or empty'
                WHEN price !~ '^-?\d*\.?\d+$'
                    THEN 'price is not numeric: ' || price
                WHEN price::numeric <= 0
                    THEN 'price is not positive: ' || price
            END,
            CASE
                WHEN tax IS NULL
                    THEN 'tax is null or empty'
                WHEN tax !~ '^-?\d*\.\d+$'
                    THEN 'tax is not numeric: ' || tax
                WHEN tax::numeric <= 0
                    THEN 'tax is not positive: ' || tax
            END
        ], NULL), '{}') AS failure_reasons

    FROM source
)

SELECT 
  * 
FROM 
  validated