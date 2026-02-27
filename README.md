# Azure Data Engineering Pipeline - NYC Taxi Green

End-to-end batch data pipeline processing NYC Green Taxi trip data 
using Azure Databricks, Delta Lake, and Unity Catalog. Implements a 
Medallion Architecture with incremental ingestion, data quality 
enforcement, dimensional modelling, and a Power BI reporting layer.

---
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
│            —  green_trips_enriched                  │
│  Joined with taxi_zone_lookup (SCD Type-2)          │
│  Pickup + dropoff borough and zone names added      │
│                                                     │
│          — taxi_zone_lookup  (SCD Type-2)           │
│  Dimension table · effective_date / end_date        │
└─────────────────────────┬───────────────────────────┘
                          │  Daily aggregation
                          ▼
┌─────────────────────────────────────────────────────┐
│  03 · GOLD  —  daily_trip_summary                   │
│  total_trips · total_revenue · avg_fare             │
│  avg_distance · avg_passengers · max/min_fare       │
└─────────────────────────┬───────────────────────────┘
                          │  Import
                          ▼
                   Power BI Dashboard
```
---
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


---

### Full Load vs Incremental Load (Batch Processing)

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

---

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

### External vs Managed Table

**Bronze and Silver → External Tables**
Data files live in ADLS Gen2 containers. Dropping a table 
removes the metadata only - the underlying Parquet/Delta files remain 
safe in storage. Protects against accidental workspace or catalog 
deletion.

**Gold → Managed Tables**
Aggregated summary tables are small and easily regenerated from Silver. 
Databricks manages the storage path. Managed Delta tables benefit from 
Databricks-optimized features such as optimized writes, and predictive 
I/O which are applied automatically without manual configuration.

**Landing → External Volume**
The landing container is also written to by ADF. Making it an External 
Volume means Databricks can read from it without owning it - dropping 
the volume does not delete the raw files ADF deposited.

---
### SCD Type-2 — Taxi Zone Lookup

<scd_type2_table.png>

`taxi_zone_lookup` is a Slowly Changing Dimension Type-2 table. When a 
zone attribute changes (borough, zone name, service zone), the existing 
active record is expired by setting `end_date`, and a new record is 
inserted with a fresh `effective_date`. Active records always have 
`end_date IS NULL`. Enrichment joins filter on this condition.

---

## Security & Storage Access

### Unity Catalog Setup

#### Microsoft Entra ID User

Azure Databricks account-level administration does not accept personal
Microsoft accounts (Gmail, Outlook personal). A dedicated user was
created in **Microsoft Entra ID** (formerly Azure AD) to serve as the
Databricks account admin — this is required for Unity Catalog
governance at the account level.

An admin group was created in the Databricks account console with this
Entra ID user added as both group member and account admin.


#### Metastore Configuration

<metastore_setup.png>

Azure creates one default metastore per region automatically, but it
has no managed storage path assigned. This means every catalog and
managed table would require an explicit storage path at creation time.

For this project the default metastore was deleted and a new one
created with a dedicated storage path:

- A container named `metastore` was created in the project storage
  account
- This path was set as the **metastore root storage** — the default
  location for all managed table data and Unity Catalog metadata
- The Databricks workspace was then assigned to this metastore

With a root path configured, managed tables (Gold layer) write to this
container automatically without specifying a path at table creation.


#### Two Access Connectors — Isolation by Design

<access_connector_setup.png>

An **Access Connector** is an Azure-managed identity that acts as the
bridge between Databricks and ADLS Gen2. Rather than using one
connector for everything, two were used for isolation:

| Connector | Assigned To | Container Access |
|---|---|---|
| `unity-catalog-access-connector` | Metastore | Metastore container only |
| `nyctaxi-data-access-connector` | Storage Credential | landing, bronze, silver, gold |

**Why two instead of one:**
The metastore connector handles Unity Catalog system metadata —
table definitions, schema information, permissions, and managed table
files. The data connector handles all pipeline data movement.

Separating them means working on Bronze or Silver
cannot accidentally touch metastore system files. If the data
connector's permissions are misconfigured or revoked, catalog metadata
remains intact. This demonstrates **Principle of Least Privilege** at
the infrastructure level — each identity has access to exactly what
it needs and nothing more.

Both connectors were assigned **Storage Blob Data Contributor** on
their respective containers via IAM Role Assignment in Azure Portal.


#### Storage Credential & External Locations

<storage_credential.png>

A **Storage Credential** is the Unity Catalog object that wraps an
Access Connector and registers it inside Databricks. Once created,
it can be referenced by External Locations and granted permissions
to users and groups — all managed centrally through Unity Catalog
rather than per-notebook configuration.
```
Azure Access Connector (nyctaxi-data-access-connector)
        │  wrapped by
        ▼
Storage Credential (nyctaxi-data-storage-credential)
        │  referenced by
        ▼
External Locations
  ├── abfss://landing@<storage-account>.dfs.core.windows.net/
  ├── abfss://bronze@<storage-account>.dfs.core.windows.net/
  ├── abfss://silver@<storage-account>.dfs.core.windows.net/
  └── abfss://gold@<storage-account>.dfs.core.windows.net/
```

**External Locations** map specific ADLS paths to the Storage
Credential. Any notebook or job in the workspace can access these
paths through Unity Catalog permissions — no credentials in code,
no authentication blocks in notebooks, no Secret Scopes required.

Permissions can be granted at three levels:
- On the Storage Credential itself
- On individual External Locations
- On catalogs, schemas, and tables built on top of those locations

This gives fine-grained access control entirely within Unity Catalog
without even touching Azure IAM.
---

### Alternative Pattern — Service Principal

<service_principal_setup.png>

A Service Principal authentication pattern is also implemented in the
codebase as a reference, with the authentication block present but
commented out in notebooks (`green_trips_raw.py`, `taxi_zone_lookup.py`).

**Why Service Principal is not the primary approach:**

Unity Catalog Storage Credentials with External Locations is the
modern Databricks-recommended pattern. Service Principal authentication
requires credentials to be fetched in every notebook at runtime,
managed per-workspace, and rotated manually. It also bypasses Unity
Catalog's centralised governance — access cannot be managed through
catalog permissions, only through Azure IAM and Key Vault.

**How it is implemented (reference):**
```
Azure Portal
  └── App Registration (sp-nyctaxi-lakehouse-dev)
        └── Client ID + Tenant ID noted
        └── Client Secret generated (visible once only)
              │
              ▼
        Key Vault (kv-nyctaxi-lake-dev)
          └── sp-client-id
          └── sp-tenant-id
          └── sp-client-secret
              │
              ▼
        Databricks Secret Scope (kv-nyctaxi-scope)
        linked to Key Vault via Vault URI + Resource ID
              │
              ▼
        Notebook
          client_secret = dbutils.secrets.get(
              scope="kv-nyctaxi-scope",
              key="sp-client-secret"
          )
          # print(client_secret) → [REDACTED]
```

The Service Principal was assigned **Storage Blob Data Contributor**
on the storage account. Credentials are never hardcoded — even
`print(secret)` outputs `[REDACTED]` in notebook output due to
Databricks secret redaction.

---

## Data Quality

### Silver Layer — Filtering & Cleansing

<data_validation_results.png>

Enforced in `green_trips_cleansed.py` before writing to Silver:

Example:
| Check | Rule | Reason |
|---|---|---|
| Late-arriving data | `year(pickup_datetime) == 2025` | Trips starting Dec 31st bundled into Jan files carry 2024 dates — invalid for 2025 reporting |
| Negative fare amounts | `fare_amount >= 0` | Negative values represent refunds, dispute reversals, and driver entry corrections — not real commercial trips |

Raw Bronze contains **1,577 negative fare records** and **6 stray 2024
records**. Without this filter, `avg_fare` and `total_revenue` in Gold
are mathematically incorrect.



### Silver Layer — Enrichment

`green_trips_enriched.py` joins `green_trips_cleansed` with
`taxi_zone_lookup` twice — once for pickup location, once for dropoff —
replacing raw integer location IDs with human-readable names:
```
pu_location_id (integer) → pu_borough, pu_zone  (e.g. Manhattan, JFK)
do_location_id (integer) → do_borough, do_zone  (e.g. Queens, Astoria)
```

Both joins use aliased DataFrames (`pu_lookup`, `do_lookup`) to avoid
column name conflicts on the same lookup table. Only active zone
records (`end_date IS NULL`) are used — ensures SCD Type-2 history
does not create duplicate join matches.



### Idempotency Validation

<idempotency_check.png>

Validated in `ad_hoc > data_validation.sql`.

Idempotency is enforced at the write layer by `upsert_delta_table()`
in `table_utils.py` — Delta MERGE matches on
`lpep_pickup_datetime + pu_location_id` and only inserts rows that
do not already exist. Re-running the pipeline any number of times
produces the same result.



### Delta Time Travel

Full version history is maintained on all Delta tables. Any prior
state can be queried for audit or recovery in `ad_hoc > data_validation.sql`.
---


## Tech Stack

| Layer | Technology |
|---|---|
| Language | Python, PySpark, SQL |
| Platform | Azure Databricks (Premium) |
| Storage | ADLS Gen2 (Medallion containers) |
| Table Format | Delta Lake |
| Catalog | Unity Catalog (Storage Credentials + External Locations) |
| Ingestion | Auto Loader (cloudFiles + checkpoint) |
| Orchestration | Azure Data Factory + Databricks Workflows |
| Security | Unity Catalog Access Connectors + Azure Key Vault (SP reference) |
| Visualisation | Power BI Desktop (DirectQuery) |
| Version Control | GitHub (Databricks Repos) |
          
---
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
        ├── data_validation.sql
        ├── green_taxi_eda.py
        └── purge_tables.py
```
