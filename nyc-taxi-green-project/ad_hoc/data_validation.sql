-- Databricks notebook source
-- MAGIC %md
-- MAGIC **Validate ingestion**

-- COMMAND ----------

-- check total rows before and after run
SELECT COUNT(*) as total_rows FROM nyctaxi.01_bronze.green_trips_raw;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Data Quality Validation**

-- COMMAND ----------

-- Verifies the filter in silver cleansed: (year == 2025) & (fare_amount >= 0)
SELECT 
    'Negative Amount' AS check_type, 
    COUNT(*) AS record_count
FROM nyctaxi.02_silver.green_trips_cleansed
WHERE fare_amount < 0

UNION ALL

SELECT 
    'Invalid year' AS check_type, 
    COUNT(*) AS record_count
FROM nyctaxi.02_silver.green_trips_cleansed
WHERE year(lpep_pickup_datetime) != 2025;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **Idempotency Check**

-- COMMAND ----------

-- INITIAL COUNT
SET var.initial_count = (SELECT COUNT(*) FROM nyctaxi.02_silver.green_trips_cleansed);

-- ReRUN NOTEBOOKS AGAIN

-- CHECK NO DUPLICATES WERE ADDED
SELECT 
    ${var.initial_count} as count_before,
    COUNT(*) as count_after,
    (COUNT(*) - ${var.initial_count}) as duplicates_added   -- result 0 - no duplicates
FROM nyctaxi.02_silver.green_trips_cleansed;

-- COMMAND ----------

SELECT COUNT(*) FROM parquet.`/Volumes/nyctaxi/00_landing/data_sources/2025-08/green_tripdata_2025-08.parquet`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ##### VERSIONING

-- COMMAND ----------

-- version history of your Gold table
DESCRIBE HISTORY nyctaxi.03_gold.daily_trip_summary;

-- Time Travel to previous versions
SELECT * FROM nyctaxi.03_gold.daily_trip_summary VERSION AS OF 1;