from pyspark.sql import SparkSession
from pyspark.sql.column import Column, _to_java_column, _to_seq
import time


start_time = time.time()

spark = SparkSession.builder \
    .appName("Scala UDF Example") \
    .config('spark.jars', 'src/resources/exo4/udf.jar') \
    .getOrCreate()

df = spark.read.csv('src/resources/exo4/sell.csv', header=True, inferSchema=True)

def addCategoryName(col):
    # Récupérer le SparkContext
    sc = spark.sparkContext
    # Via sc._jvm accéder à des fonctions Scala
    add_category_name_udf = sc._jvm.fr.hymaia.sparkfordev.udf.Exo4.addCategoryNameCol()
    # Retourner un objet colonne avec l'application de notre udf Scala
    return Column(add_category_name_udf.apply(_to_seq(sc, [col], _to_java_column)))

df = df.withColumn('category_name', addCategoryName(df['category']))

df.show()

spark.stop()

end_time = time.time()
print("Temps d'exécution : {:.2f} secondes".format(end_time - start_time))