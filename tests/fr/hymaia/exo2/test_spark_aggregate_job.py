from tests.fr.hymaia.spark_test_case import spark
import unittest
import pyspark.sql.functions as f
# from src.fr.hymaia.exo1.main import clean
from pyspark.sql import Row
import test_spark_aggregate_job

def agg(df):
    return df.groupBy(f.col("departement")).agg(f.count(f.col(("name"))).alias("nb_people"))


class TestMain(unittest.TestCase):
     def test_agg(self):
        # GIVEN
        input = spark.createDataFrame([
            Row(name='Toto', departement='2A'),
            Row(name='Bob', departement='2A'),
            Row(name='Roro', departement='2B'),
            Row(name='Bibi', departement='75'),
        ])

        expected = spark.createDataFrame([
            Row(departement='2A', nb_people=2),
            Row(departement='2B', nb_people=1),
            Row(departement='75', nb_people=1)
        ])

        # WHEN
        actual = agg(input)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())
