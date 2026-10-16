
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

DELTA_PATH = "s3a://earthquake-data/delta_test/"

spark = SparkSession.builder \
    .appName("DeltaLakeUpdateTest") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Open the existing Delta table
delta_table = DeltaTable.forPath(spark, DELTA_PATH)

# Update the magnitude of one event
delta_table.update(
    condition="event_id = 'delta001'",
    set={"magnitude": "4.5"}
)

# Read the updated table
result = spark.read.format("delta").load(DELTA_PATH)

print("Updated Delta Lake table:")
result.show(truncate=False)

spark.stop()
