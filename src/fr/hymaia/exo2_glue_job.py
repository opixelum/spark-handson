import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
# TODO : import custom spark code dependencies

if __name__ == '__main__':
    spark = SparkSession.builder.getOrCreate()
    glueContext = GlueContext(spark.sparkContext)
    job = Job(glueContext)
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "PARAM_1", "PARAM_2"])
    job.init(args['JOB_NAME'], args)

    param1 = args["PARAM_1"]
    param2 = args["PARAM_2"]

    # TODO : call function to run spark transformations

    job.commit()