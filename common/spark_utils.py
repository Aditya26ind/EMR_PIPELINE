from pyspark.sql import SparkSession


def get_spark_session():
    return SparkSession.builder.appName("EMR Pipeline").getOrCreate()