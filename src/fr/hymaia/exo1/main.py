import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession



def wordcount(df, col_name):
    return df.withColumn('word', f.explode(f.split(f.col(col_name), ' '))) \
        .groupBy('word') \
        .count()

def main():
    # creer une app spark avec une session
    spark = SparkSession.builder \
    .appName("wordcount") \
    .master("local[*]") \
    .getOrCreate() 

    df = spark.read.option('header', True).csv('src/resources/exo1/data.csv')
    d7 = wordcount(df, "text")
    d7.write.partitionBy("count").parquet("data/exo1/output", mode = "overwrite")

    


