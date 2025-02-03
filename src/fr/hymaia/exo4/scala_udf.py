from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.column import Column, _to_java_column, _to_seq
from time import perf_counter


spark = SparkSession.builder \
    .appName("exo4") \
    .master("local[*]") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()


def add_category_name(col):
    # on récupère le SparkContext
    sc = spark.sparkContext
    
    # Via sc._jvm on peut accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4 \
        .addCategoryNameCol()
    
    # On retourne un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(
        _to_seq(sc, [col], _to_java_column)
    ))


def main():
    sell_path: str = "src/resources/exo4/sell.csv"
    print(f"Reading {sell_path}...")
    sell_df: DataFrame = spark.read.csv(sell_path, header=True)

    print("Adding category names...")
    named_categories_df: DataFrame = sell_df.withColumn(  # Définition de la procédure
        "category_name",
        add_category_name(sell_df.category)
    )
    start: float = perf_counter()
    named_categories_df.count()
    end: float = perf_counter()
    total_time: float = round(end - start, 3)
    print(f"Done in {total_time} seconds.")

    named_categories_path: str = "data/exo4/scala_udf_named_categories.csv"
    print(f"Writing dataframe to {named_categories_path}...")
    named_categories_df.write.parquet(named_categories_path, mode="overwrite")

    spark.stop()
