-- =============================================================
-- BRONZE LAYER: Business Daily Events (Multiplex JSON)
-- Source: v02/business_daily_events (7 JSON files, daily drops)
-- Each file has mixed event_group: store_ops, marketing, logistics
-- Pattern: 1 Raw Streaming Table + 3 Routed Materialized Views
-- =============================================================

-- ---- Raw Business Events (all event groups) ----
CREATE OR REFRESH STREAMING TABLE bronze_business_events_raw
COMMENT 'Raw multiplex business events. Contains store_ops, marketing_campaign, logistics_fulfillment events.'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT
  event_id,
  event_group,
  event_type,
  event_timestamp,
  subsidiary,
  region,
  payload,
  _metadata.file_name AS source_file,
  _metadata.file_modification_time AS file_loaded_at
FROM STREAM read_files(
  '/Volumes/databricks_simulated_retail_customer_data/v02/business_daily_events',
  format => 'json',
  schemaEvolutionMode => 'addNewColumns'
);

-- ---- Store Ops Events (Routed View) ----
CREATE OR REFRESH MATERIALIZED VIEW bronze_store_ops_events
COMMENT 'Store operations events routed from bronze_business_events_raw'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT * FROM STREAM(bronze_business_events_raw)
WHERE event_group = 'store_ops';

-- ---- Marketing Campaign Events (Routed View) ----
CREATE OR REFRESH MATERIALIZED VIEW bronze_marketing_events
COMMENT 'Marketing campaign events routed from bronze_business_events_raw'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT * FROM STREAM(bronze_business_events_raw)
WHERE event_group = 'marketing_campaign';

-- ---- Logistics Fulfillment Events (Routed View) ----
CREATE OR REFRESH MATERIALIZED VIEW bronze_logistics_events
COMMENT 'Logistics fulfillment events routed from bronze_business_events_raw'
TBLPROPERTIES ('quality' = 'bronze')
AS
SELECT * FROM STREAM(bronze_business_events_raw)
WHERE event_group = 'logistics_fulfillment';
