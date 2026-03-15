-- =============================================================
-- BRONZE LAYER: Customer Changes Daily (CDC Feed)
-- Source: v02/customer_changes_daily (7 JSON files, daily drops)
-- Pattern: Streaming Table via read_files (Auto Loader)
-- =============================================================

CREATE OR REFRESH STREAMING TABLE bronze_customer_changes
COMMENT 'Raw customer CDC events from customer_changes_daily volume. Ingested via Auto Loader.'
TBLPROPERTIES (
  'quality' = 'bronze',
  'delta.enableChangeDataFeed' = 'true'
)
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
  operation,         -- insert / update / delete
  timestamp,         -- event time for CDC ordering
  _metadata.file_name  AS source_file,
  _metadata.file_modification_time AS file_loaded_at
FROM STREAM read_files(
  '/Volumes/databricks_simulated_retail_customer_data/v02/customer_changes_daily',
  format => 'json',
  schemaEvolutionMode => 'addNewColumns'
);
