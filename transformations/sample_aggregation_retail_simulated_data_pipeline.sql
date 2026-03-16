-- =============================================================
-- SAMPLE: Aggregation Queries on Gold Layer
-- Purpose: Business KPI reporting from gold_fact_orders
-- =============================================================

-- Total Revenue by Subsidiary and Month
SELECT
  subsidiary,
  DATE_TRUNC('month', order_date)             AS order_month,
  COUNT(DISTINCT order_id)                    AS total_orders,
  SUM(qty)                                    AS total_units_sold,
  ROUND(SUM(net_amount), 2)                   AS total_net_revenue,
  ROUND(AVG(discount_pct) * 100, 2)           AS avg_discount_pct
FROM gold_fact_orders
GROUP BY subsidiary, DATE_TRUNC('month', order_date)
ORDER BY order_month DESC, total_net_revenue DESC;

-- Revenue by Category and Channel
SELECT
  category,
  channel,
  ROUND(SUM(net_amount), 2)                   AS net_revenue,
  COUNT(DISTINCT order_id)                    AS order_count,
  ROUND(SUM(net_amount) / COUNT(DISTINCT order_id), 2) AS avg_order_value
FROM gold_fact_orders
GROUP BY category, channel
ORDER BY net_revenue DESC;

-- Top 10 Customers by Revenue
SELECT
  f.customer_id,
  c.first_name,
  c.last_name,
  c.loyalty_tier,
  COUNT(DISTINCT f.order_id)                  AS total_orders,
  ROUND(SUM(f.net_amount), 2)                 AS lifetime_value
FROM gold_fact_orders f
JOIN gold_dim_customer_current c
  ON f.customer_id = c.customer_id
GROUP BY f.customer_id, c.first_name, c.last_name, c.loyalty_tier
ORDER BY lifetime_value DESC
LIMIT 10;
