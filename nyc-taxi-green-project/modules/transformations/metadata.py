from pyspark.sql import DataFrame
from pyspark.sql.functions import current_timestamp

# input -> Dataframe, output -> returns Dataframe with current timestamp
def add_processed_timestamp(df: DataFrame) -> DataFrame:

    return df.withColumn("processed_timestamp", current_timestamp())