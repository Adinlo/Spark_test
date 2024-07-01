import pyspark.sql.functions as f
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import time


start_time = time.time()

spark = SparkSession.builder \
    .appName("Python UDF Example") \
    .getOrCreate()

df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)

def get_category_name(category):
    if category < 6:
        return "food"
    else:
        return "furniture"

get_category_name_udf = udf(get_category_name, StringType())

df = df.withColumn('category_name', get_category_name_udf(df['category']))

df.show()

spark.stop()

end_time = time.time()
print("Temps d'exécution : {:.2f} secondes".format(end_time - start_time))