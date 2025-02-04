import pyspark.sql.functions as f
from pyspark.sql import DataFrame


def clean(clients_df: DataFrame, cities_df: DataFrame) -> DataFrame:
    clients_df: DataFrame = only_adults(clients_df)
    joined_df: DataFrame = join_clients_cities(clients_df, cities_df)
    with_department_df: DataFrame = add_department_column(joined_df)

    return with_department_df


def only_adults(clients: DataFrame) -> DataFrame:
    return clients.filter(clients.age >= 18)


def join_clients_cities(clients: DataFrame, cities: DataFrame) -> DataFrame:
    return (clients.join(cities, "zip")
            .select(clients.name, clients.age, clients.zip, cities.city))

def add_department_column(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "department",
        f.when(
            f.substring(df.zip, 0, 2) == "20",
            f.when(df.zip <= 20190, "2A").otherwise("2B")
        ).otherwise(f.substring(df.zip, 0, 2))
    )
