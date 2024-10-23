import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame

"""
- Que signifie `local[*]` ?
  Cela signifie que Spark va tourner localement sur le plus de threads
  possible

- Pourquoi avoir choisi `local[*]` et non pas simplement `local` ?
  Parce que `local` ne permet pas de paralléliser les calculs.

- Les avantages du format parquet sont :
  - Données compressées
  - Stockage binaire
  - Lecture rapide
"""

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("wordcount") \
    .getOrCreate()


def wordcount(df: DataFrame, col_name: str) -> DataFrame:
    return df.withColumn(
        'word',
        f.explode(f.split(f.col(col_name), pattern=' '))
    ).groupBy('word').count()


def main():
    df_path: str = "src/resources/exo1/data.csv"
    print(f"Reading {df_path}...")
    df: DataFrame = spark.read.csv(df_path, header=True)

    print("Counting words and adding column...")
    df: DataFrame = wordcount(df, col_name="text")

    output_path: str = "data/exo1/output"
    print(f"Writing dataframe to {output_path}...")
    df.write.partitionBy("count").parquet(output_path, mode="overwrite")

    print("Done.")
    spark.stop()
