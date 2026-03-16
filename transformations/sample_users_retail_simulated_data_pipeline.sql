-- =============================================================
-- SAMPLE: User / Customer Queries on Silver & Gold Layer
-- Purpose: Customer insights and loyalty analysis
-- =============================================================

-- Customer Count by Loyalty Tier
SELECT
  loyalty_tier,
  COUNT(DISTINCT customer_id)                 AS customer_count,
  ROUND(COUNT(DISTINCT customer_id) * 100.0 /
    SUM(COUNT(DISTINCT customer_id)) OVER (), 2) AS pct_of_total
FROM gold_dim_customer_current
GROUP BY loyalty_tier
ORDER BY customer_count DESC;

-- Customers Who Changed Loyalty Tier (SCD2 History)
SELECT
  customer_id,
  first_name,
  last_name,
  loyalty_tier,
  __START_AT                                  AS valid_from,
  __END_AT                                    AS valid_to
FROM silver_dim_customer
WHERE customer_id IN (
  SELECT customer_id
  FROM silver_dim_customer
  GROUP BY customer_id
  HAVING COUNT(*) > 1
)
ORDER BY customer_id, valid_from;

-- New Customers by Month (First Order Date)
SELECT
  DATE_TRUNC('month', MIN(order_date))        AS acquisition_month,
  COUNT(DISTINCT customer_id)                 AS new_customers
FROM silver_orders
GROUP BY customer_id
HAVING MIN(order_date) = MAX(order_date)
ORDER BY acquisition_month;
