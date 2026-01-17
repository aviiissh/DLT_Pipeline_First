-- Orders Pipeline with Data Quality Expectations
-- This pipeline demonstrates adding data quality expectations to a Lakeflow Spark Declarative Pipeline

-- Step 1: Create Bronze Streaming Table with Data Quality Expectations
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.orders_bronze_demo3
  (
    -- Add data quality expectations here
    -- Example: CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
  )
  COMMENT "Ingest raw JSON order files from cloud storage with data quality checks"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false
  )
AS 
SELECT 
  *,
  current_timestamp() AS processing_time, 
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/orders", 
  format => "json");

-- Step 2: Create Silver Streaming Table with Data Quality Expectations
CREATE OR REFRESH STREAMING TABLE ldp_demo.ldp_schema.orders_silver_demo3
  (
    -- Add data quality expectations here
    -- Example: 
    -- CONSTRAINT valid_timestamp EXPECT (order_timestamp > "2021-12-25") ON VIOLATION DROP ROW,
    -- CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION FAIL UPDATE
  )
  COMMENT "Cleaned and transformed orders data with quality checks"
  TBLPROPERTIES ("quality" = "silver")
AS 
SELECT
  order_id,
  timestamp(order_timestamp) AS order_timestamp, 
  customer_id,
  notifications
FROM STREAM ldp_demo.ldp_schema.orders_bronze_demo3;

-- Step 3: Create Gold Materialized View
CREATE OR REFRESH MATERIALIZED VIEW ldp_demo.ldp_schema.orders_by_date_gold_demo3
  COMMENT "Daily order aggregations"
  TBLPROPERTIES ("quality" = "gold")
AS 
SELECT 
  date(order_timestamp) AS order_date, 
  count(*) AS total_daily_orders
FROM ldp_demo.ldp_schema.orders_silver_demo3                               
GROUP BY date(order_timestamp);

