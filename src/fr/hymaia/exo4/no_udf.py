from pyspark.sql import DataFrame, SparkSession, Window
from time import perf_counter
import pyspark.sql.functions as f

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("no_udf") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

def main():
    sell_path: str = "src/resources/exo4/sell.csv"
    print(f"Reading {sell_path}...")
    sell_df: DataFrame = spark.read.csv(sell_path, header=True)

    print("Adding category names...")
    named_categories_df: DataFrame = sell_df.withColumn(
        "category_name",
        f.when(sell_df.category < 6, "food").otherwise("furniture")
    )
    start: float = perf_counter()
    named_categories_df.count()
    end: float = perf_counter()
    total_time: float = round(end - start, 3)
    print(f"Done in {total_time} seconds.")

    named_categories_path: str = "data/exo4/no_udf_named_categories.csv"
    print(f"Writing dataframe to {named_categories_path}...")
    named_categories_df.write.parquet(named_categories_path, mode="overwrite")

    # Price per category per day
    print("Calculating total price per category per day...")
    window = Window.partitionBy("date", "category").orderBy("id")
    total_price_per_category_per_day_df = named_categories_df.withColumn(
        "total_price_per_category_per_day",
        f.sum("price").over(window)
    )

    total_price_per_category_per_day_path: str = "data/exo4/total_price_per_category_per_day.parquet"
    print(f"Writing dataframe to {total_price_per_category_per_day_path}...")
    total_price_per_category_per_day_df.write.parquet(
        total_price_per_category_per_day_path,
        mode="overwrite"
    )

    # Price per category per day
    print("Calculating total price per category per day last 30 days...")
    window_30_days = Window.partitionBy("category") \
        .orderBy(f.unix_timestamp(f.col("date"), "yyyy-MM-dd")) \
        .rangeBetween(-30 * 86400, 0) # 86 400 = seconds in a day
    total_price_per_category_per_day_last_30_days_df = named_categories_df.withColumn(
        "total_price_per_category_per_day_last_30_days",
        f.sum("price").over(window_30_days)
    )

    total_price_per_category_per_day_last_30_days_path: str = "data/exo4/total_price_per_category_per_day_last_30_days.parquet"
    print(f"Writing dataframe to {total_price_per_category_per_day_last_30_days_path}...")
    total_price_per_category_per_day_last_30_days_df.write.parquet(
        total_price_per_category_per_day_last_30_days_path,
        mode="overwrite"
    )

    spark.stop()
