import os
from pyspark.sql import SparkSession

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

spark = SparkSession.builder \
    .appName("unit test") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "3") \
    .getOrCreate()
