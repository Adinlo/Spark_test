from tests.fr.hymaia.spark_test_case import spark
import unittest
from src.fr.hymaia.exo2.spark_clean_job import filter
from src.fr.hymaia.exo2.spark_clean_job import city_join_client
from src.fr.hymaia.exo2.spark_clean_job import add_department
from pyspark.sql import Row


class TestMain(unittest.TestCase):
    def test_clean(self):
        # GIVEN
        input = spark.createDataFrame(
            [
                Row(name='Turing', age=21, zip='75000', city='Paris' ),
                Row(name='Bob', age=41, zip='94140', city='Alfortville' ),
                Row(name='Gogo', age=81, zip='75000', city='Paris' ),
                Row(name='Roro', age=19, zip='75000', city='Paris' ),
            ]
        )
        expected = spark.createDataFrame(
            [
                Row(name='Turing', age=21, zip='75000', city='Paris' ),
                Row(name='Bob', age=41, zip='94140', city='Alfortville' ),
                Row(name='Gogo', age=81, zip='75000', city='Paris' ),
                
            ]
        )
        # when
        actual = filter(input, '20')

        # then
        self.assertCountEqual(actual.collect(), expected.collect())


    def test_clean2(self):
        # GIVEN

        df2 = spark.createDataFrame([
            Row(zip='75000', city='Paris'),
            Row(zip='94140', city='Alfortiville'),
        ])

        df1 = spark.createDataFrame([
            Row(zip='75000', name='Turing', age=21 ),
            Row(zip='94140', name='Bob', age=41 ),
        ])

        expected = spark.createDataFrame([
                Row(ziip='75000', city='Paris', name='Turing', age=21),
                Row(ziip='94140', city='Alfortiville', name='Bob', age=41)
            
            ])

        # when
        actual = city_join_client(df2, df1)

        # then
        self.assertEqual(actual.collect(), expected.collect())


    def test_clean3(self):
        # GIVEN
        df = spark.createDataFrame([
            Row(zip='75000'),
            Row(zip='20100'),
            Row(zip='20600')
        ])
        expected = spark.createDataFrame([
            Row(zip='75000', department='75'),
            Row(zip='20100', department='2A'),
            Row(zip='20600', department='2B')
        ])

        # WHEN
        actual = add_department(df)

        # THEN
        self.assertCountEqual(actual.collect(), expected.collect())


    def test_INT(self):
      # Given
      input_1 = spark.createDataFrame([
         Row(name="Toto", age=18, zip=94140),
         Row(name="Coco", age=28, zip=75000)
      ])
      input_2 = spark.createDataFrame([
         Row(zip='94140', city='Alfortville'),
         Row(zip='75000', city="Paris")
      ])
      # When
      actual1 = filter(input_1, 20)
      actual2 = city_join_client(input_2, actual1)
      actual = add_department(actual2)
      # Then
      output = spark.createDataFrame([
         Row(zip='75000', city='Paris', name='Coco', age=28, department='75')
      ])

      self.assertCountEqual(actual.collect(), output.collect())

    
