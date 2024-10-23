from src.fr.hymaia.exo2.aggregate.aggregate import *
from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row, DataFrame

class TestAggregate(unittest.TestCase):
    def test_only_adults(self):
        # Given
        data: DataFrame = spark.createDataFrame([
            Row(name="John", age=67, zip=10001),
            Row(name="Jane", age=46, zip=20000),
            Row(name="James", age=16, zip=20600),
            Row(name="Jack", age=24, zip=20600)
        ])
        expected: DataFrame = spark.createDataFrame([
            Row(name="John", age=67, zip=10001),
            Row(name="Jane", age=46, zip=20000),
            Row(name="Jack", age=24, zip=20600)
        ])

        # When
        actual: DataFrame = only_adults(data)

        # Then
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_join_clients_cities(self):
        # Given
        clients_data: DataFrame = spark.createDataFrame([
            Row(name="John", age=67, zip=10001),
            Row(name="Jane", age=46, zip=20000),
            Row(name="Jack", age=24, zip=20600)
        ])
        cities_data: DataFrame = spark.createDataFrame([
            Row(zip=10001, city="New York"),
            Row(zip=20000, city="Ajaccio"),
            Row(zip=20600, city="Bastia"),
        ])
        expected: DataFrame = spark.createDataFrame([
            Row(name="John", age=67, zip=10001, city="New York"),
            Row(name="Jane", age=46, zip=20000, city="Ajaccio"),
            Row(name="Jack", age=24, zip=20600, city="Bastia")
        ])

        # When
        actual: DataFrame = join_clients_cities(clients_data, cities_data)

        # Then
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_add_department_column(self):
        # Given
        data: DataFrame = spark.createDataFrame([
            Row(name="John", age=67, zip=10001, city="New York"),
            Row(name="Jane", age=46, zip=20000, city="Ajaccio"),
            Row(name="Jack", age=24, zip=20600, city="Bastia")
        ])
        expected: DataFrame = spark.createDataFrame([
            Row(name="John", age=67, zip=10001, city="New York", department="10"),
            Row(name="Jane", age=46, zip=20000, city="Ajaccio", department="2A"),
            Row(name="Jack", age=24, zip=20600, city="Bastia", department="2B")
        ])

        # When
        actual: DataFrame = add_department_column(data)

        # Then
        self.assertCountEqual(actual.collect(), expected.collect())
