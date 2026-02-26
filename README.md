# Azure Data Engineering Pipeline - NYC Taxi Green

End-to-end batch data pipeline processing NYC Green Taxi trip data 
using Azure Databricks, Delta Lake, and Unity Catalog. Implements a 
Medallion Architecture with incremental ingestion, data quality 
enforcement, dimensional modelling, and a Power BI reporting layer.

Built as a portfolio project demonstrating production-grade data 
engineering patterns on the Azure stack.


## Architecture

NYC TLC Public CDN (parquet / CSV) 
(https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
                          │
                          │  HTTP download via urllib / ADF Copy Activity
                          ▼
┌─────────────────────────────────────────────────────┐
│  00 · LANDING                                       │
│  Databricks Volume / ADLS Gen2                      │
│  Raw parquet files partitioned by yyyy-MM           │
└─────────────────────────┬───────────────────────────┘
                          │  Auto Loader (cloudFiles)
                          ▼
┌─────────────────────────────────────────────────────┐
│  01 · BRONZE  —  green_trips_raw                    │
│  Raw Delta table · load_timestamp · source_file     │
│  Idempotent via checkpoint                          │
└─────────────────────────┬───────────────────────────┘
                          │  Cleanse + filter
                          ▼
┌─────────────────────────────────────────────────────┐
│  02 · SILVER  —  green_trips_cleansed               │
│  2025 data only · fare ≥ 0 · disputes removed       │
│  Decoded vendor / rate / payment columns            │
│                                                     │
│            green_trips_enriched                     │
│  Joined with taxi_zone_lookup (SCD Type-2)          │
│  Pickup + dropoff borough and zone names added      │
│                                                     │
│            taxi_zone_lookup  (SCD Type-2)           │
│  Dimension table · effective_date / end_date        │
└─────────────────────────┬───────────────────────────┘
                          │  Daily aggregation
                          ▼
┌─────────────────────────────────────────────────────┐
│  03 · GOLD  —  daily_trip_summary                   │
│  total_trips · total_revenue · avg_fare             │
│  avg_distance · avg_passengers · max/min_fare       │
└─────────────────────────┬───────────────────────────┘
                          │ Cleanse + filter
                          ▼
                          │  DirectQuery
                          ▼
                   Power BI Dashboard
