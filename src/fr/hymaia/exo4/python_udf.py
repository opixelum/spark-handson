from pyspark.sql import SparkSession, DataFrame
from time import perf_counter
import pyspark.sql.functions as f


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("python_udf") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()


@f.udf("string")
def add_category_name(int_category: int) -> str:
    return "food" if int(int_category) < 6 else "furniture"


def main():
    sell_path: str = "src/resources/exo4/sell.csv"
    print(f"Reading {sell_path}...")
    sell_df: DataFrame = spark.read.csv(sell_path, header=True)

    print("Adding category names...")
    named_categories_df: DataFrame = sell_df.withColumn(
        "category_name",
        add_category_name(sell_df.category)
    )

    named_categories_path: str = "data/exo4/python_udf_named_categories.parquet"
    print(f"Writing dataframe to {named_categories_path}...")
    start: float = perf_counter()
    named_categories_df.write.parquet(named_categories_path, mode="overwrite")
    end: float = perf_counter()
    total_time: float = round(end - start, 3)
    print(f"Done in {total_time} seconds.")

    spark.stop()
