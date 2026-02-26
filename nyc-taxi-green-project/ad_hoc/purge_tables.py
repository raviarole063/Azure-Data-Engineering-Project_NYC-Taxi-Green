# Databricks notebook source
# Update this date to the date you want to delete data from
date_from = 'yyyy-MM-dd'

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.01_bronze.green_trips_raw")

dt.delete(f"lpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.02_silver.green_trips_cleansed")

dt.delete(f"lpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.02_silver.green_trips_enriched")

dt.delete(f"lpep_pickup_datetime >= '{date_from}'")

# COMMAND ----------

from delta.tables import DeltaTable

dt = DeltaTable.forName(spark, "nyctaxi.03_gold.daily_trip_summary")

dt.delete(f"pickup_date >= '{date_from}'")

# COMMAND ----------

# %sql

# DROP TABLE IF EXISTS nyctaxi.01_bronze.green_trips_raw;

# DROP TABLE IF EXISTS nyctaxi.02_silver.green_trips_cleansed;
# DROP TABLE IF EXISTS nyctaxi.02_silver.green_trips_enriched;
# DROP TABLE IF EXISTS nyctaxi.02_silver.taxi_zone_lookup;

# DROP TABLE IF EXISTS nyctaxi.03_gold.daily_trip_summary;

# COMMAND ----------

# %sql

# DROP SCHEMA IF EXISTS nyctaxi.01_bronze CASCADE;
# DROP SCHEMA IF EXISTS nyctaxi.02_silver CASCADE;
# DROP SCHEMA IF EXISTS nyctaxi.03_gold CASCADE;