-- =============================================================
-- SILVER LAYER: Cleaned & Enriched Orders
-- Source: bronze_all_orders (unified streaming table)
-- Pattern: Materialized View with dedup, cleaning, enrichment
-- =============================================================

CREATE OR REFRESH MATERIALIZED VIEW silver_orders
COMMENT 'Cleaned, deduplicated and enriched orders from all 3 subsidiaries.'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.enableChangeDataFeed' = 'true'
)
AS
SELECT
  order_id,
  order_timestamp,
  CAST(order_date AS DATE)                    AS order_date,
  customer_id,
  subsidiary,
  region,
  country,
  city,
  channel,
  sku,
  category,
  qty,
  unit_price,
  COALESCE(discount_pct, 0)                   AS discount_pct,
  coupon_code,
  ROUND(qty * unit_price * (1 - COALESCE(discount_pct, 0)), 2) AS net_amount,
  total_amount,
  source_file,
  CURRENT_TIMESTAMP()                          AS silver_loaded_at
FROM (
  SELECT *,
    ROW_NUMBER() OVER (
      PARTITION BY order_id
      ORDER BY order_timestamp DESC
    ) AS rn
  FROM STREAM(bronze_all_orders)
  WHERE order_id IS NOT NULL
    AND customer_id IS NOT NULL
    AND qty > 0
    AND unit_price > 0
)
WHERE rn = 1;
