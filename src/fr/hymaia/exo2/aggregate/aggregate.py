from pyspark.sql import DataFrame
from pyspark.sql import functions as f


def aggregate(df: DataFrame) -> DataFrame:
    dept_count_df: DataFrame = count_clients_per_department(df)
    sorted_df: DataFrame = sort_by_clients_per_department(dept_count_df)

    return sorted_df


def count_clients_per_department(df: DataFrame) -> DataFrame:
    return df.groupBy("department").agg(
        f.countDistinct("name").alias("nb_people")
    )


def sort_by_clients_per_department(df: DataFrame) -> DataFrame:
    return df.orderBy(
        f.desc("nb_people"), f.asc("department")
    )
