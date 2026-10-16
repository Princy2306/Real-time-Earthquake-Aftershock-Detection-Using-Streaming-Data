
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

PATH = "s3a://earthquake-data/silver_delta_integration/"

spark = (
    SparkSession.builder
    .appName("VerifySilverDuplicates")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.format("delta").load(PATH)

print("Total Silver records:", df.count())
print("Distinct event IDs:", df.select("event_id").distinct().count())

duplicates = (
    df.groupBy("event_id")
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

print("Duplicate event IDs:")
duplicates.show(truncate=False)

spark.stop()
