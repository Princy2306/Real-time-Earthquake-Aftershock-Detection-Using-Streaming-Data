
from pyspark.sql import SparkSession
from delta.tables import DeltaTable

DELTA_PATH = "s3a://earthquake-data/silver_delta_test/"

spark = (
    SparkSession.builder
    .appName("EarthquakeDeltaMergeTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Simulate incoming earthquake events.
# delta001 already exists; delta004 is a new event.
incoming_data = [
    ("delta001", 4.8, "Updated test location A"),
    ("delta004", 3.5, "New test location D")
]

columns = ["event_id", "magnitude", "place"]

incoming_df = spark.createDataFrame(incoming_data, columns)

delta_table = DeltaTable.forPath(spark, DELTA_PATH)

# Update existing events and insert new events.
(
    delta_table.alias("target")
    .merge(incoming_df.alias("source"), "target.event_id = source.event_id")
    .whenMatchedUpdate(set={
        "magnitude": "source.magnitude",
        "place": "source.place"
    })
    .whenNotMatchedInsert(values={
        "event_id": "source.event_id",
        "magnitude": "source.magnitude",
        "place": "source.place"
    })
    .execute()
)

# Verify the result
result = spark.read.format("delta").load(DELTA_PATH)

print("Delta MERGE test completed!")
print("Total records:", result.count())
result.orderBy("event_id").show(truncate=False)

spark.stop()
