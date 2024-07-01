import sys

from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as f
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from spark_clean_job import main as cleanMain
from spark_aggregate_job import main as aggregateMain


if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PARAM_1", "PARAM_2"])
    job.init(args['JOB_NAME'], args)

    param1 = args["PARAM_1"]
    param2 = args["PARAM_2"]

    # TODO : call function to run spark transformations
    cleanMain(spark)
    aggregateMain(spark)

    job.commit()