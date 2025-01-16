from src.fr.hymaia.exo2.aggregate.aggregate import *
from tests.fr.hymaia.spark_test_case import spark
import unittest
from pyspark.sql import Row, DataFrame
from pyspark.testing import assertDataFrameEqual
from pyspark.sql.utils import AnalysisException


class TestAggregate(unittest.TestCase):
    def test_count_clients_per_department(self):
        # Given
        data: DataFrame = spark.createDataFrame([
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
        expected: DataFrame = spark.createDataFrame([
            Row(department=26, nb_people=1),
            Row(department=25, nb_people=2),
        ])

        # When
        actual: DataFrame = count_clients_per_department(data)

        # Then
        assertDataFrameEqual(actual, expected)

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
        assertDataFrameEqual(actual, expected)

    def test_count_clients_per_department_expect_error_missing_col(self):
        # Missing `department` column
        data_missing_dept: DataFrame = spark.createDataFrame([
            Row(name='Fullilove', age=42, zip=26150, city="SOLAURE EN DIOIS"),
            Row(name='Mccatty', age=18, zip=25650, city="VILLE DU PONT"),
        ])

        with self.assertRaises(AnalysisException):
            count_clients_per_department(data_missing_dept)
