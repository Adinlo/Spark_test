# import pyspark.sql.functions as f
# from pyspark.sql import SparkSession

# def agg(df):
#     return df.groupBy(f.col("departement")).agg(f.count(f.col(("name"))).alias("nb_people"))

# def main():
#     spark = SparkSession.builder \
#     .appName("output") \
#     .master("local[*]") \
#     .getOrCreate() 

#     df = spark.read.option('header', True).parquet('data/exo2/clean')
#     df_agg = agg(df)
    
#     df_agg.show()
#     df_agg.write.mode("overwrite").parquet("data/exo2/aggregate")

#     import pyspark.sql.functions as f
# from pyspark.sql import SparkSession


def main(spark):
    # spark = (
    #     SparkSession.builder.appName("aggregate_job").master("local[*]").getOrCreate()
    # )

    df_clean = spark.read.option("header", True).parquet("s3://spark-cotututoto/data/exo2/clean")

    res = aggregate_population_by_departement(df_clean)
    res.coalesce(1).write.mode("overwrite").option("header", True).csv(
        "s3://spark-cotututoto/data/exo2/aggregate"
    )


def aggregate_population_by_departement(df):
    return get_population_by_departement(df)


def get_population_by_departement(df):
    return (
        df.groupBy("departement")
        .count()
        .withColumnRenamed("count", "nb_people")
        .orderBy(f.desc("nb_people"), f.col("departement"))
    )