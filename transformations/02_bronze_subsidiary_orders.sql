-- =============================================================
-- BRONZE LAYER: Subsidiary Daily Orders (3 Subsidiaries)
-- Sources:
--   bright_home_orders     -> CSV format (home goods)
--   lumina_sports_orders   -> CSV format (outdoor/athletic)
--   northstar_outfitters_orders -> JSON format (camping/travel)
-- Pattern: 3 Streaming Tables + 1 Unified Materialized View
-- =============================================================

-- ---- Bright Home Orders (CSV) ----
CREATE OR REFRESH STREAMING TABLE bronze_bright_home_orders
COMMENT 'Raw orders from Bright Home subsidiary - CSV daily drops'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  subsidiary_id, order_id, order_timestamp, customer_id,
  region, country, city, channel, sku, category,
  qty, unit_price, discount_pct, coupon_code,
  total_amount, order_date,
  'bright_home' AS subsidiary,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/databricks_simulated_retail_customer_data/v02/subsidiary_daily_orders/bright_home_orders',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);

-- ---- Lumina Sports Orders (CSV) ----
CREATE OR REFRESH STREAMING TABLE bronze_lumina_sports_orders
COMMENT 'Raw orders from Lumina Sports subsidiary - CSV daily drops'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  subsidiary_id, order_id, order_timestamp, customer_id,
  region, country, city, channel, sku, category,
  qty, unit_price, discount_pct, coupon_code,
  total_amount, order_date,
  'lumina_sports' AS subsidiary,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/databricks_simulated_retail_customer_data/v02/subsidiary_daily_orders/lumina_sports_orders',
  format => 'csv',
  header => 'true',
  inferSchema => 'true'
);

-- ---- Northstar Outfitters Orders (JSON) ----
CREATE OR REFRESH STREAMING TABLE bronze_northstar_outfitters_orders
COMMENT 'Raw orders from Northstar Outfitters subsidiary - JSON daily drops'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  subsidiary_id, order_id, order_timestamp, customer_id,
  region, country, city, channel, sku, category,
  qty, unit_price, discount_pct, coupon_code,
  total_amount, order_date,
  'northstar_outfitters' AS subsidiary,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  '/Volumes/databricks_simulated_retail_customer_data/v02/subsidiary_daily_orders/northstar_outfitters_orders',
  format => 'json'
);

-- ---- Unified Bronze Orders View (all 3 subsidiaries) ----
CREATE OR REFRESH STREAMING TABLE bronze_all_orders
COMMENT 'Unified view of all 3 subsidiary orders. Schema-normalized via UNION ALL.'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT * FROM STREAM(bronze_bright_home_orders)
UNION ALL
SELECT * FROM STREAM(bronze_lumina_sports_orders)
UNION ALL
SELECT * FROM STREAM(bronze_northstar_outfitters_orders);
