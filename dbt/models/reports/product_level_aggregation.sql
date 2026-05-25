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
    dim.product_id AS product_id,
    dim.product_name AS product_name,
    COUNT(fct.transaction_id) AS total_transactions,
    COUNT(DISTINCT fct.customer_id) AS unique_customers,
    SUM(fct.quantity) AS total_units_sold,
    ROUND(SUM(fct.price * fct.quantity), 2) AS total_revenue,
    ROUND(SUM(fct.tax), 2) AS total_tax,
    ROUND(SUM(fct.total_amount), 2) AS total_revenue_with_tax,
    ROUND(AVG(fct.total_amount), 2) AS avg_transaction_value
FROM 
  fct
  LEFT JOIN dim 
    ON fct.product_id = dim.product_id
GROUP BY 
  dim.product_id, 
  dim.product_name
ORDER BY 
  total_revenue DESC