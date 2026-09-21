from pyspark.sql import SparkSession
from pyspark.sql.functions import when

spark = SparkSession.builder \
    .appName("EarthquakeClassification") \
    .getOrCreate()

data = [
    ("eq001", 4.2),
    ("eq002", 5.8),
    ("eq003", 3.9),
    ("eq004", 6.4),
    ("eq005", 5.1),
    ("eq006", 4.9)
]

columns = ["earthquake_id", "magnitude"]

df = spark.createDataFrame(data, columns)

result = df.withColumn(
    "magnitude_category",
    when(df.magnitude < 4, "Weak")
    .when(df.magnitude < 5, "Moderate")
    .when(df.magnitude < 6, "Strong")
    .otherwise("Major")
)

selected = result.select(
    "earthquake_id",
    "magnitude",
    "magnitude_category"
)


selected.show()

spark.stop()