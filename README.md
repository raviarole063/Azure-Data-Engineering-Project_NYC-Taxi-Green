# Azure Data Engineering Pipeline - NYC Taxi Green

End-to-end batch data pipeline processing NYC Green Taxi trip data 
using Azure Databricks, Delta Lake, and Unity Catalog. Implements a 
Medallion Architecture with incremental ingestion, data quality 
enforcement, dimensional modelling, and a Power BI reporting layer.

Built as a portfolio project demonstrating production-grade data 
engineering patterns on the Azure stack.


## Architecture

**Source:** [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
```
NYC TLC Public CDN  (parquet / CSV)
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
│  2025 data only · fare >= 0 · disputes removed      │
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
                          │  DirectQuery
                          ▼
                   Power BI Dashboard
```

## How It Works

### Orchestration Flow

<pipeline_overview.png>

The pipeline accepts two parameters at runtime:

| Parameter | Type | Example — Backfill | Example — Incremental |
|---|---|---|---|
| `p_month_start` | Integer | 5 (May) | 11 (November) |
| `p_month_end` | Integer | 10 (October) | 11 (November) |

**ADF runs three activities in sequence:**

**1. Green Trips Copy Activity (ForEach)**
Loops over every month integer from `p_month_start` to `p_month_end`.
Each iteration constructs the NYC TLC URL dynamically using the year
and zero-padded month, downloads the parquet file, and lands it in
ADLS Gen2 under `/00_landing/nyctaxi_green/{yyyy-MM}/`.

Passing `p_month_start=5, p_month_end=10` downloads 6 files.
Passing `p_month_start=11, p_month_end=11` downloads one file for
the incremental monthly run.

**2. Taxi Zone Lookup Copy Activity**
Single Copy Activity — no loop. Downloads the zone lookup CSV from
the NYC TLC CDN and lands it in `/00_landing/lookup/`. Runs
independently of the green trips loop.

**3. Databricks Job Trigger (Web Activity)**
After both Copy Activities complete, ADF calls the Databricks Jobs
REST API to trigger the transformation job. The pipeline parameters
`p_month_start` and `p_month_end` are forwarded as notebook
parameters in the request body.

<adf_web_activity_config.png>

---
### Parameter Flow — ADF to Databricks

<adf_pipeline_params.png>

The parameters travel end-to-end from the ADF trigger down to the
Silver filtering logic:
```
ADF Pipeline
  p_month_start = 11
  p_month_end   = 11
        │
        │  passed in Web Activity body
        ▼
Databricks Job
        │
        │  received via dbutils.widgets
        ▼
green_trips_cleansed.py
  p_start_month = dbutils.widgets.get("p_start_month")  →  11
  p_end_month   = dbutils.widgets.get("p_end_month")    →  11

  df_cleansed.filter()
```

---

### Databricks Job — Task Dependencies

<databricks_job_dag.png>

The job is triggered by ADF after Copy Activities complete. Parameters
`p_month_start` and `p_month_end` are passed from the ADF pipeline and
received in notebooks via `dbutils.widgets`.

Two tracks run in parallel then merge at enrichment:
```
Track A — Green Trips          Track B — Taxi Zone Lookup
──────────────────────         ──────────────────────────
01_green_trips_raw             taxi_zone_lookup (SCD Type-2)
        ↓                                  │
02_silver_trips_cleansed                   │
(p_month_start / p_month_end               │
 filter applied here)                      │
        ↓                                  │
03_silver_trips_enriched ←─────────────────┘
        ↓
04_daily_trips_summary
```
---

### Option B — Separate `docs/adf-orchestration.md`

Keep the main README clean and create:
```
nyc-taxi-project/
  └── docs/
        ├── adf-orchestration.md     ← full ADF pipeline details
        └── databricks-job.md        ← job config, params, cluster
```

---

### Full Load vs Incremental Load

Handled automatically by `get_filtered_dataframe()` in `table_utils.py`.
No manual flags required — the function inspects the target table state
at runtime and switches mode accordingly.

| Condition | Behaviour |
|---|---|
| Target table empty or does not exist | **Full load** — reads entire Bronze table |
| Target table has data | **Incremental** — reads only the month window passed from ADF (`p_month_start` to `p_month_end`) |

Both are batch operations. The distinction is only in **how much data
is read from Bronze** on a given run:

- **First run** — table is empty, all available months are processed
- **Subsequent runs** — only the months specified by ADF parameters
  are read and merged, avoiding reprocessing historical data

The month window is configurable at pipeline trigger time via
`p_month_start` and `p_month_end`. The default delay between data
availability and processing is adjustable in `date_utils.py` —
`get_month_start_n_months_ago(n)` where `n` can be changed to match
the NYC TLC release schedule.


## Unity Catalog — Storage Architecture

<unity_catalog_storage.png>

Unity Catalog governs all data access. The catalog `nyctaxi` contains 
four schemas mapping directly to the Medallion layers.

### Catalog Structure
```
nyctaxi (catalog)
  ├── 00_landing  — External Volume → abfss://landing@...
  ├── 01_bronze   — External Tables  → abfss://bronze@...
  ├── 02_silver   — External Tables  → abfss://silver@...
  └── 03_gold     — Managed Tables   → Metastore root
```

### External vs Managed Table Strategy

**Bronze and Silver → External Tables**
Data files live in your own ADLS Gen2 containers. Dropping a table 
removes the metadata only — the underlying Parquet/Delta files remain 
safe in storage. Protects against accidental workspace or catalog 
deletion.

**Gold → Managed Tables**
Aggregated summary tables are small and easily regenerated from Silver. 
Databricks manages the storage path. Simplifies access for Power BI.

**Landing → External Volume**
The landing container is also written to by ADF. Making it an External 
Volume means Databricks can read from it without owning it — dropping 
the volume does not delete the raw files ADF deposited.

### SCD Type-2 — Taxi Zone Lookup

<scd_type2_table.png>

`taxi_zone_lookup` is a Slowly Changing Dimension Type-2 table. When a 
zone attribute changes (borough, zone name, service zone), the existing 
active record is expired by setting `end_date`, and a new record is 
inserted with a fresh `effective_date`. Active records always have 
`end_date IS NULL`. Enrichment joins filter on this condition.


## Security — Service Principal Authentication

<service_principal_setup.png>

Storage access uses a **Service Principal** (App Registration) rather 
than account keys, demonstrating the **Principle of Least Privilege**.

**Flow:**
```
Databricks notebook
        ↓  dbutils.secrets.get(scope, key)
Azure Key Vault  (kv-nyctaxi-scope)
        ↓  OAuth token exchange
Service Principal  (sp-nyctaxi-lakehouse-dev)
        ↓  Storage Blob Data Contributor role
ADLS Gen2  (stnyctaxigreen)
```

Credentials are never hardcoded. The Databricks Secret Scope acts as 
the bridge between notebooks and Key Vault — even `print(secret)` 
outputs `[REDACTED]` in notebook output.

Two Access Connectors are used:

| Connector | Permission | Purpose |
|---|---|---|
| unity-catalog-access-connector | Metastore container only | Unity Catalog system metadata |
| nyctaxi-data-access-connector | landing/bronze/silver/gold | Data layer access |

This isolates metastore system files from data processing — a data 
engineer cannot accidentally delete catalog metadata by touching the 
data containers.


## Data Quality

<data_validation_results.png>

Enforced in the Silver layer via `green_trips_cleansed.py`:

| Check | Rule | Rows Affected |
|---|---|---|
| Late-arriving data | `year(pickup) == 2025` | 6 stray 2024 records removed |
| Negative fares | `fare_amount >= 0` | 1,577 reversals/disputes removed |
| Invalid payment types | Exclude Dispute, No charge | Ensures avg_fare reflects real rides |

**Idempotency validation** — `data_validation.sql` captures row count 
before a re-run and confirms zero rows added after. Delta MERGE in 
`upsert_delta_table()` ensures this holds at the write layer.

**Delta Time Travel** — full version history on Gold table. Can query 
any prior state with `VERSION AS OF`.


## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python, PySpark, SQL |
| Platform | Azure Databricks (Premium) |
| Storage | ADLS Gen2 (Medallion containers) |
| Table Format | Delta Lake |
| Catalog | Unity Catalog |
| Ingestion | Auto Loader (cloudFiles) |
| Orchestration | Databricks Workflows + Azure Data Factory |
| Security | Azure Key Vault + Service Principal + Secret Scope |
| Visualisation | Power BI Desktop (DirectQuery) |
| Version Control | GitHub (Databricks Repos) |
| IaC | ADF ARM Template export |


## Repository Structure
```
nyc-taxi-project/
  ├── transformations/
  │     └── notebooks/
  │           ├── 00_landing/          # ingest_green_trips, ingest_lookup
  │           ├── 01_bronze/           # green_trips_raw (Auto Loader)
  │           ├── 02_silver/           # cleansed, enriched, taxi_zone_lookup
  │           └── 03_gold/             # daily_trip_summary
  ├── modules/
  │     ├── data_loader/               # file_downloader.py
  │     ├── transformations/           # metadata.py
  │     └── utils/                     # date_utils.py, table_utils.py
  ├── setup/
  │     └── create_catalog_and_schemas.sql
  ├── ad_hoc/
  │     ├── data_validation.sql
  │     ├── green_taxi_eda.py
  │     └── purge_tables.py
  └── adf/                             # ADF ARM template + pipeline JSON
```
