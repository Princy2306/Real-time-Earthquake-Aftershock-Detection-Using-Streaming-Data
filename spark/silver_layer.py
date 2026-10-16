
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
    trim
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)

# MinIO configuration
MINIO_USER = os.environ["MINIO_ROOT_USER"]
MINIO_PASSWORD = os.environ["MINIO_ROOT_PASSWORD"]

BRONZE_PATH = "s3a://earthquake-data/bronze/"
SILVER_PATH = "s3a://earthquake-data/silver/"

spark = (
    SparkSession.builder
    .appName("EarthquakeSilverLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define the structure of each earthquake JSON message
earthquake_schema = StructType([
    StructField("id", StringType(), True),
    StructField("magnitude", DoubleType(), True),
    StructField("place", StringType(), True),
    StructField("time", LongType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("depth", DoubleType(), True),
    StructField("source", StringType(), True)
])

# Read raw Bronze data from MinIO
bronze_df = spark.read.parquet(BRONZE_PATH)

# Parse the JSON contained in raw_message
parsed_df = bronze_df.withColumn(
    "event",
    from_json(col("raw_message"), earthquake_schema)
)

# Extract fields into individual columns
silver_df = parsed_df.select(
    col("event.id").alias("event_id"),
    col("event.magnitude").alias("magnitude"),
    trim(col("event.place")).alias("place"),
    col("event.time").alias("event_time_ms"),
    col("event.longitude").alias("longitude"),
    col("event.latitude").alias("latitude"),
    col("event.depth").alias("depth_km"),
    col("event.source").alias("source"),
    col("kafka_timestamp")
)

# Convert event time from milliseconds to a timestamp
silver_df = silver_df.withColumn(
    "event_time",
    to_timestamp(
        (col("event_time_ms") / 1000).cast("timestamp")
    )
)

# Remove records missing essential fields
silver_df = silver_df.filter(
    col("event_id").isNotNull()
    & col("magnitude").isNotNull()
    & col("latitude").isNotNull()
    & col("longitude").isNotNull()
    & col("event_time").isNotNull()
)

# Validate coordinate and magnitude ranges
silver_df = silver_df.filter(
    col("latitude").between(-90, 90)
    & col("longitude").between(-180, 180)
    & col("magnitude").between(-2, 10)
)

# Keep one record per event ID
silver_df = silver_df.dropDuplicates(["event_id"])

# Write cleaned data to Silver in Parquet format
silver_df.write.mode("overwrite").parquet(SILVER_PATH)

print("Silver layer created successfully!")
print("Silver record count:", silver_df.count())
silver_df.show(20, truncate=False)
silver_df.printSchema()

spark.stop()
