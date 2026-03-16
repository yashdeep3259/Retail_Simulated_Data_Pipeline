-- =============================================================
-- SILVER LAYER: Customer Dimension (SCD Type 2)
-- Source: bronze_customer_changes (CDC streaming table)
-- Pattern: APPLY CHANGES INTO for SCD Type 2 tracking
-- =============================================================

CREATE OR REFRESH STREAMING TABLE silver_dim_customer
COMMENT 'SCD Type 2 customer dimension. Tracks historical changes to customer attributes.'
TBLPROPERTIES (
  'quality' = 'silver',
  'delta.enableChangeDataFeed' = 'true'
);

APPLY CHANGES INTO silver_dim_customer
FROM STREAM(bronze_customer_changes)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'delete'
SEQUENCE BY timestamp
STORED AS SCD TYPE 2
TRACK HISTORY ON *
EXCEPT (source_file, file_loaded_at, operation, timestamp);
