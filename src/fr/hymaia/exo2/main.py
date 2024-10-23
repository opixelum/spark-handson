from .aggregate import aggregate
from .clean import clean
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

    # Aggregate job
    agg_df: DataFrame = aggregate.aggregate(clients_df, cities_df)
    output_path: str = "data/exo2/output"
    print(f"Writing aggregate dataframe in {output_path}...")
    agg_df.write.parquet(output_path, mode="overwrite")

    # Clean job
    clean_df: DataFrame = clean.clean(agg_df)
    output_path: str = "data/exo2/aggregate"
    print(f"Writing clean dataframe to {output_path}...")
    clean_df.coalesce(1).write.csv(
        output_path,
        header=True,
        mode="overwrite"
    )

    print("Done.")
    spark.stop()
