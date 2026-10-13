
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("EarthquakeBronzeStream")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "earthquakes")
    .option("startingOffsets", "earliest")
    .load()
)

raw_df = kafka_df.select(
    col("value").cast("string").alias("raw_message"),
    col("timestamp").alias("kafka_timestamp")
)

raw_df.printSchema()




query = (
    raw_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "s3a://earthquake-data/bronze/"
    )
    .option(
        "checkpointLocation",
        "s3a://earthquake-data/checkpoints/bronze/"
    )
    .start()
)

query.awaitTermination()
