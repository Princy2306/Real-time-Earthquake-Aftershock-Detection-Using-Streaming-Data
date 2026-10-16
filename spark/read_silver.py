
import os
from pyspark.sql import SparkSession

SILVER_PATH = "s3a://earthquake-data/silver/"

spark = (
    SparkSession.builder
    .appName("ReadSilverLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

silver_df = spark.read.parquet(SILVER_PATH)

print("Silver record count:", silver_df.count())
silver_df.show(20, truncate=False)
silver_df.printSchema()

spark.stop()
