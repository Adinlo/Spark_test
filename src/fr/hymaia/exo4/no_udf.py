from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, sum as spark_sum
from pyspark.sql.window import Window
import time

start_time = time.time()

spark = SparkSession.builder \
    .appName("No UDF Example") \
    .getOrCreate()

df = spark.read.csv("src/resources/exo4/sell.csv", header=True, inferSchema=True)
df = df.withColumn("category_name", when(col("category") < 6, "food").otherwise("furniture"))
df.show()

window_fun = Window.partitionBy("date", "category")
df = df.withColumn("total_price_per_category_per_day", spark_sum("price").over(window_fun))
df.show()


window_30_days = Window.partitionBy("category").orderBy("date").rowsBetween(-29, 0)
df = df.withColumn("total_price_per_category_per_day_last_30_days", spark_sum("price").over(window_30_days))
df.show()

spark.stop()

end_time = time.time()
print("Temps d'exécution : {:.2f} secondes".format(end_time - start_time))