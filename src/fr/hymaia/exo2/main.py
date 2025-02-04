from .clean import clean
from .aggregate import aggregate
from pyspark.sql import DataFrame, SparkSession


spark = SparkSession.builder \
    .master("local[*]") \
    .appName("city-clients") \
    .getOrCreate()


def main():
    clients_df: DataFrame = spark.read.csv(
        "src/resources/exo2/clients_bdd.csv",
        header=True
    )
    cities_df: DataFrame = spark.read.csv(
        "src/resources/exo2/city_zipcode.csv",
        header=True
    )

    # Clean job
    clean_df: DataFrame = clean.clean(clients_df, cities_df)
    output_path: str = "data/exo2/output"
    clean_df.write.parquet(output_path, mode="overwrite")

    # Aggregate job
    agg_df: DataFrame = aggregate.aggregate(clean_df)
    output_path: str = "data/exo2/aggregate"
    agg_df.coalesce(1).write.csv(
        output_path,
        header=True,
        mode="overwrite"
    )

    spark.stop()
