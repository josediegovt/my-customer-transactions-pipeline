WITH 
  fct AS (
    SELECT 
      * 
    FROM 
      {{ ref('fct_customer_transactions') }}
)

SELECT
    transaction_date,
    COUNT(transaction_id) AS total_transactions,
    COUNT(DISTINCT customer_id) AS unique_customers,
    SUM(quantity) AS total_units_sold,
    ROUND(SUM(price * quantity), 2) AS total_revenue,
    ROUND(SUM(tax), 2) AS total_tax,
    ROUND(SUM(total_amount), 2) AS total_revenue_with_tax,
    ROUND(AVG(total_amount), 2) AS avg_transaction_value
FROM 
  fct
GROUP BY 
  transaction_date
ORDER BY 
  transaction_date ASC