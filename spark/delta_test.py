
from pyspark.sql import SparkSession

DELTA_PATH = "s3a://earthquake-data/delta_test/"

spark = (
    SparkSession.builder
    .appName("DeltaLakeMinIOTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Create a few sample earthquake records
data = [
    ("delta001", 4.2, "Test location A"),
    ("delta002", 3.8, "Test location B"),
    ("delta003", 5.1, "Test location C"),
]

columns = ["event_id", "magnitude", "place"]
df = spark.createDataFrame(data, columns)

# Write a Delta table to MinIO
df.write.format("delta").mode("overwrite").save(DELTA_PATH)

# Read the Delta table back
result = spark.read.format("delta").load(DELTA_PATH)

print("Delta Lake test succeeded!")
print("Record count:", result.count())
result.show(truncate=False)

spark.stop()
