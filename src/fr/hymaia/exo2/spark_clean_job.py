import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def filter(df, age):
    return df.filter(f.col("age") >= age)


def city_join_client (df2, df1):
    return df2.join(df1, "zip", "inner")


def add_department (df3):
   return df3.withColumn(
        "department",
                   f.when(
                        f.substring(df3["zip"], 1, 2) != "20", f.substring(df3["zip"], 1, 2))
                         .otherwise(f.when(f.col("zip") <= 20190, "2A").otherwise("2B"))
    )


def main(): #show
    # creer une app spark avec une session
    spark = SparkSession.builder \
    .appName("yee") \
    .master("local[*]") \
    .getOrCreate() 

    df1 = spark.read.option('header', True).csv('src/resources/exo2/clients_bdd.csv')

    df2 = spark.read.option('header', True).csv('src/resources/exo2/city_zipcode.csv') \
    
    clientfilter = filter(df1, 18)
    df3 = city_join_client(df2, df1)
    dp = add_department(df3)

    # dp.show()
    dp.write.mode("overwrite").parquet("data/exo2/clean")
    # clientfilter.show()
    # df3.show()

