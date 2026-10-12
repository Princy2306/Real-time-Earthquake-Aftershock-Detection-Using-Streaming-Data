
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = (
    SparkSession.builder
    .appName("KafkaStreamTest")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "earthquakes")
    .option("startingOffsets", "earliest")
    .option("maxOffsetsPerTrigger", 5)
    .load()
)

messages = kafka_df.select(
    col("value").cast("string").alias("message")
)

query = (
    messages.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)

query.awaitTermination()
