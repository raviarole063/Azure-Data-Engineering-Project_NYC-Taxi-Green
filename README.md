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


## How It Works

### Orchestration Flow

<pipeline_overview.png>

Azure Data Factory runs two Copy Activities in sequence:

1. **Green Trips Copy** — ForEach loop over months [1–12] for a given 
   year. Each iteration downloads one parquet file from the NYC TLC CDN 
   and lands it in ADLS Gen2. Skips months already present in storage. 
   Silently skips future months that return 404. Zero-pads month numbers 
   to match URL format (09 not 9).

2. **Taxi Zone Lookup Copy** — Single Copy Activity. Downloads the zone 
   lookup CSV from NYC TLC and lands it in ADLS.

After both Copy Activities complete, ADF fires the Databricks Job via 
Web Activity (REST API call), which runs the full transformation 
pipeline.

<adf_pipeline_canvas.png>

---

### Databricks Job — Task Dependencies

<databricks_job_dag.png>

The job runs two tracks in parallel then merges:

**Track A — Green Trips**
```
00_ingest_green_trips
        ↓
continue_downstream_yellow_taxi  (condition: taskValue == "yes")
        ↓ true
01_green_trips_raw
        ↓
02_green_trips_cleaned
        ↓
02_green_trips_enriched ←─────────────┐
        ↓                             │
03_daily_trip_summary          Track B joins here
```

**Track B — Taxi Zone Lookup**
```
00_ingest_lookup
        ↓
continue_downstream_lookup  (condition: taskValue == "yes")
        ↓ true
02_taxi_zone_lookup ──────────────────┘
```

If ingestion finds the file already exists, `continue_downstream` is 
set to `"no"` and all downstream tasks are automatically skipped. 
No wasted compute.

---

### Full Load vs Incremental Load

Handled automatically by `get_filtered_dataframe()` in `table_utils.py`:

| Condition | Behaviour |
|---|---|
| Target table does not exist or is empty | **Full load** — reads all data from source |
| Target table has data | **Incremental** — reads only the 3-months-ago window |

The 3-month delay matches the NYC TLC data release schedule. 
`get_month_start_n_months_ago(3)` in `date_utils.py` calculates the 
target window dynamically at runtime — no hardcoded dates.

No manual flags. No if/else branching in notebooks. The utility 
function detects the state of the target table and switches mode 
automatically.
