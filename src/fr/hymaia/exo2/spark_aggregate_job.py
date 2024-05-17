import pyspark.sql.functions as f
from pyspark.sql import SparkSession

def agg(df):
    return df.groupBy(f.col("departement")).agg(f.count(f.col(("name"))).alias("nb_people"))

def main():
    spark = SparkSession.builder \
    .appName("output") \
    .master("local[*]") \
    .getOrCreate() 

    df = spark.read.option('header', True).parquet('data/exo2/clean')
    df_agg = agg(df)
    
    df_agg.show()
    df_agg.write.mode("overwrite").parquet("data/exo2/aggregate")