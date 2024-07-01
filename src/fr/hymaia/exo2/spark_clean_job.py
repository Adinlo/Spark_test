# import pyspark.sql.functions as f
# from pyspark.sql import SparkSession


# def filter(df, age):
#     return df.filter(f.col("age") >= age)


# def city_join_client (df2, df1):
#     return df2.join(df1, "zip", "inner")


# def add_department (df3):
#    return df3.withColumn(
#         "department",
#                    f.when(
#                         f.substring(df3["zip"], 1, 2) != "20", f.substring(df3["zip"], 1, 2))
#                          .otherwise(f.when(f.col("zip") <= 20190, "2A").otherwise("2B"))
#     )


# def main(): 
#     spark = SparkSession.builder \
#     .appName("yee") \
#     .master("local[*]") \
#     .getOrCreate() 

#     df1 = spark.read.option('header', True).csv('src/resources/exo2/clients_bdd.csv')

#     df2 = spark.read.option('header', True).csv('src/resources/exo2/city_zipcode.csv') \
    
#     clientfilter = filter(df1, 18)
#     df3 = city_join_client(df2, df1)
#     dp = add_department(df3)

#     dp.write.mode("overwrite").parquet("data/exo2/clean")
  

import pyspark.sql.functions as f
from pyspark.sql import SparkSession


def main(spark):
    # spark = SparkSession.builder.appName("clean_job").master("local[*]").getOrCreate()

    df_city = spark.read.option("header", True).csv(
        "s3://spark-cotututoto/src/resources/exo2/city_zipcode.csv"
    )
    df_clients = (
        spark.read.option("header", True)
        .csv("s3://spark-cotututoto/src/resources/exo2/clients_bdd.csv")
        .withColumnRenamed("zip", "zip_clients")
    )

    res = get_adult_and_city(df_clients, df_city)
    res.write.mode("overwrite").parquet("s3://spark-cotututoto/data/exo2/clean")


def get_adult_and_city(df_clients, df_city):
    df_clients_majeurs = filter_by_age(df_clients)
    df_clients_majeurs_joined = join_by_zip(
        df_clients_majeurs, "zip_clients", df_city, "zip"
    )
    res = add_departement(df_clients_majeurs_joined)

    return res


def filter_by_age(df):
    return df.filter(df.age > 18)


def join_by_zip(df_client, df_client_column, df_city, df_city_column):
    return df_client.join(
        df_city, df_client[df_client_column] == df_city[df_city_column], "left"
    ).drop(df_client_column)


def add_departement(df):
    return df.withColumn(
        "departement",
        f.when((f.col("zip") <= 20190) & (f.col("zip").startswith("20")), "2A")
        .when((f.col("zip") > 20190) & (f.col("zip").startswith("20")), "2B")
        .otherwise(f.substring("zip", 1, 2)),
    )