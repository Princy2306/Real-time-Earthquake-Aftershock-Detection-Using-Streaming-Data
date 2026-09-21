from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkMinIOTest") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "admin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

data = [
    ("eq001", 4.5, 26.2, 73.0),
    ("eq002", 5.8, 35.6, 139.7),
    ("eq003", 6.2, 34.1, 118.2)
]

columns = [
    "earthquake_id",
    "magnitude",
    "latitude",
    "longitude"
]

df = spark.createDataFrame(data, columns)

df.show()

df.write \
    .mode("overwrite") \
    .parquet("s3a://earthquake-data/test/")

print("Data successfully written to MinIO!")

spark.stop()