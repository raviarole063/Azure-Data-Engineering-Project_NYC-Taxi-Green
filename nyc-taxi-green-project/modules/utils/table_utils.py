from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col
from delta.tables import DeltaTable

def get_filtered_dataframe(spark: SparkSession, source_table: str, target_table: str, 
                           start_date: str, end_date: str, date_col: str = "lpep_pickup_datetime"):
    
    """Determines if a Full or Incremental read is needed."""
    
    is_target_empty = True
    if spark.catalog.tableExists(target_table):

        if spark.read.table(target_table).limit(1).count() > 0:
            is_target_empty = False

    # Read from source
    if source_table.startswith("/Volumes/"):
        df_source = spark.read.format("parquet").load(source_table)
    else:
        df_source = spark.read.table(source_table)

    if is_target_empty:
        print(f"--- [FULL LOAD] --- Reading all data for {target_table}")
        return df_source
    else:
        print(f"--- [INCREMENTAL] --- Reading data for {start_date} to {end_date}")
        return df_source.filter((col(date_col) >= start_date) & (col(date_col) < end_date))


def upsert_delta_table(spark: SparkSession, df: DataFrame, target_table: str, 
                        merge_condition: str, storage_path: str = None):
    """
    Handles both External and Managed Table creation in Unity Catalog.
    - If storage_path is provided -> Created as EXTERNAL.
    - If storage_path is None -> Created as MANAGED.
    """
    
    if not spark.catalog.tableExists(target_table):
        if storage_path:
            print(f"Creating EXTERNAL table: {target_table} at {storage_path}")
            df.write.format("delta") \
              .option("path", storage_path) \
              .mode("overwrite") \
              .saveAsTable(target_table)
        else:
            print(f"Creating MANAGED table: {target_table}")    # Managed tables automatically go to the Metastore Root Managed Location
            df.write.format("delta") \
              .mode("overwrite") \
              .saveAsTable(target_table)
    else:
        print(f"Merging data into {target_table}")
        dt = DeltaTable.forName(spark, target_table)
        dt.alias("t").merge(
            df.alias("s"), 
            merge_condition
        ).whenNotMatchedInsertAll().execute()