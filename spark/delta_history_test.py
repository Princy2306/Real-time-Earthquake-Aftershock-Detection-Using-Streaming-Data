
from pyspark.sql import SparkSession

DELTA_PATH = "s3a://earthquake-data/delta_test/"

spark = (
    SparkSession.builder
    .appName("DeltaLakeHistoryTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

history = (
    spark.sql(f"DESCRIBE HISTORY delta.`{DELTA_PATH}`")
)

history.select(
    "version",
    "timestamp",
    "operation"
).show(truncate=False)

spark.stop()

