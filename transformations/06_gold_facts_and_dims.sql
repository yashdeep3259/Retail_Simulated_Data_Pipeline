-- =============================================================
-- GOLD LAYER: Facts & Dimensions for Analytics
-- Pattern: Materialized Views joining Silver tables
-- Includes: fact_orders, dim_customer, dim_date, dim_product
-- =============================================================

-- ---- Fact Table: Orders ----
CREATE OR REFRESH MATERIALIZED VIEW gold_fact_orders
COMMENT 'Gold fact table for order transactions. Joins silver_orders with customer dimension.'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  o.order_id,
  o.order_date,
  o.order_timestamp,
  o.customer_id,
  o.subsidiary,
  o.region,
  o.country,
  o.city,
  o.channel,
  o.sku,
  o.category,
  o.qty,
  o.unit_price,
  o.discount_pct,
  o.coupon_code,
  o.net_amount,
  o.total_amount,
  c.__START_AT                                AS customer_valid_from,
  c.__END_AT                                  AS customer_valid_to,
  c.loyalty_tier,
  c.source_subsidiary                         AS customer_subsidiary
FROM silver_orders o
LEFT JOIN silver_dim_customer c
  ON o.customer_id = c.customer_id
  AND o.order_timestamp BETWEEN c.__START_AT AND COALESCE(c.__END_AT, TIMESTAMP('9999-12-31'));

-- ---- Dim Table: Customer (Current State) ----
CREATE OR REFRESH MATERIALIZED VIEW gold_dim_customer_current
COMMENT 'Current state of customers - latest record per customer_id from SCD2 dimension.'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  customer_id,
  first_name,
  last_name,
  email,
  city,
  signup_date,
  source_subsidiary,
  loyalty_tier,
  __START_AT                                  AS effective_from
FROM silver_dim_customer
WHERE __END_AT IS NULL;

-- ---- Dim Table: Date ----
CREATE OR REFRESH MATERIALIZED VIEW gold_dim_date
COMMENT 'Date dimension derived from silver_orders date range.'
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT DISTINCT
  order_date                                  AS date_key,
  YEAR(order_date)                            AS year,
  QUARTER(order_date)                         AS quarter,
  MONTH(order_date)                           AS month,
  DAYOFMONTH(order_date)                      AS day,
  DAYOFWEEK(order_date)                       AS day_of_week,
  DATE_FORMAT(order_date, 'MMMM')             AS month_name,
  DATE_FORMAT(order_date, 'EEEE')             AS day_name
FROM silver_orders;
