from src.fr.hymaia.exo2.clean.clean import *
from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row, DataFrame

base_data: DataFrame = spark.createDataFrame([
    Row(
        name='Fullilove',
        age=42,
        zip=26150,
        city="SOLAURE EN DIOIS",
        department=26
    ),
    Row(
        name='Mccatty',
        age=18,
        zip=25650,
        city="VILLE DU PONT",
        department=25
    ),
    Row(
        name='Delara',
        age=43,
        zip=25640,
        city="VILLERS GRELOT",
        department=25
    ),
])


class TestClean(unittest.TestCase):
    def test_count_clients_per_department(self):
        # Given
        data: DataFrame = base_data
        expected: DataFrame = spark.createDataFrame([
            Row(department=26, nb_people=1),
            Row(department=25, nb_people=2),
        ])

        # When
        actual: DataFrame = count_clients_per_department(data)

        # Then
        self.assertCountEqual(actual.collect(), expected.collect())

    def test_sort_by_clients_per_department(self):
        # Given
        data: DataFrame = spark.createDataFrame([
            Row(department=26, nb_people=1),
            Row(department=25, nb_people=2),
        ])
        expected: DataFrame = spark.createDataFrame([
            Row(department=25, nb_people=2),
            Row(department=26, nb_people=1),
        ])

        # When
        actual: DataFrame = sort_by_clients_per_department(data)

        # Then
        self.assertEqual(actual.collect(), expected.collect())

    def test_integration(self):
        # Given
        data: DataFrame = base_data
        expected: DataFrame = spark.createDataFrame([
            Row(department=25, nb_people=2),
            Row(department=26, nb_people=1),
        ])

        # When
        actual: DataFrame = clean(data)

        # Then
        self.assertEqual(actual.collect(), expected.collect())