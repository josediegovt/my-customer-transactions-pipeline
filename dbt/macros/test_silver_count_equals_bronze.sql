-- this test/macro is not working as expected, but will leave it to discuss it

{% macro test_silver_count_equals_bronze(model, column_name) %}

WITH bronze AS (
    SELECT COUNT(*) AS total FROM bronze.customer_transactions
),

clean AS (
    SELECT COUNT(*) AS total FROM {{ model }}
),

quarantine AS (
    SELECT COUNT(*) AS total FROM {{ ref('customer_transactions_quarantine') }}
)

SELECT
    bronze.total AS bronze_count,
    clean.total AS clean_count,
    quarantine.total  AS quarantine_count,
    clean.total + quarantine.total AS silver_total
FROM bronze
CROSS JOIN clean
CROSS JOIN quarantine
WHERE bronze.total != clean.total + quarantine.total

{% endmacro %}