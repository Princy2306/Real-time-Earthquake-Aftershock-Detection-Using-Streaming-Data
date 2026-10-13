
import os
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("ReadBronze")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["MINIO_ROOT_USER"]
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["MINIO_ROOT_PASSWORD"]
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
)

df = spark.read.parquet("s3a://earthquake-data/bronze/")

df.show(truncate=False)
df.printSchema()

spark.stop()
