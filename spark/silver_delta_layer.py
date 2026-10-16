
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    trim,
    timestamp_millis
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    LongType
)
from delta.tables import DeltaTable

BRONZE_PATH = "s3a://earthquake-data/bronze/"
DELTA_SILVER_PATH = "s3a://earthquake-data/silver_delta_integration/"

spark = (
    SparkSession.builder
    .appName("EarthquakeSilverDeltaLayer")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Define the earthquake JSON schema
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

# Read raw Bronze data
bronze_df = spark.read.parquet(BRONZE_PATH)

# Parse earthquake JSON
parsed_df = bronze_df.withColumn(
    "event",
    from_json(col("raw_message"), earthquake_schema)
)

# Extract structured fields
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

# Convert milliseconds to timestamp
silver_df = silver_df.withColumn(
    "event_time",
    timestamp_millis(col("event_time_ms"))
)

# Validate required fields
silver_df = silver_df.filter(
    col("event_id").isNotNull()
    & col("magnitude").isNotNull()
    & col("latitude").isNotNull()
    & col("longitude").isNotNull()
    & col("event_time").isNotNull()
)

# Validate ranges
silver_df = silver_df.filter(
    col("latitude").between(-90, 90)
    & col("longitude").between(-180, 180)
    & col("magnitude").between(-2, 10)
)

# Remove duplicate event IDs within this batch
silver_df = silver_df.dropDuplicates(["event_id"])

# Create the Delta table on the first run.
# On later runs, update matching events and insert new ones.
if not DeltaTable.isDeltaTable(spark, DELTA_SILVER_PATH):
    silver_df.write.format("delta").mode("errorifexists").save(
        DELTA_SILVER_PATH
    )
else:
    delta_table = DeltaTable.forPath(spark, DELTA_SILVER_PATH)

    (
        delta_table.alias("target")
        .merge(
            silver_df.alias("source"),
            "target.event_id = source.event_id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

# Read and verify the resulting Delta table
result = spark.read.format("delta").load(DELTA_SILVER_PATH)

print("Silver Delta layer completed successfully!")
print("Silver record count:", result.count())
result.orderBy("event_time").show(20, truncate=False)
result.printSchema()

spark.stop()
