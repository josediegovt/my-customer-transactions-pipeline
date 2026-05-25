WITH 
  fct AS (
    SELECT 
      * 
    FROM 
      {{ ref('fct_customer_transactions') }}
), 
  dim AS (
    SELECT 
      * 
    FROM 
      {{ ref('dim_product') }}
)

SELECT
    fct.customer_id AS customer_id,
    COUNT(fct.transaction_id) AS total_transactions,
    SUM(fct.quantity) AS total_units_purchased,
    ROUND(SUM(fct.price * fct.quantity), 2) AS total_revenue,
    ROUND(SUM(fct.tax), 2) AS total_tax,
    ROUND(SUM(fct.total_amount), 2) AS total_spent,
    ROUND(AVG(fct.total_amount), 2) AS avg_transaction_value,
    MIN(fct.transaction_date) AS first_transaction_date,
    MAX(fct.transaction_date) AS last_transaction_date
FROM fct
  LEFT JOIN dim 
    ON fct.product_id = dim.product_id
GROUP BY 
  fct.customer_id
ORDER BY 
  total_spent DESC