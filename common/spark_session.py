from pyspark.sql import SparkSession


def get_spark(app_name="EMR Pipeline"):
    """
    Create or get an existing SparkSession.
    
    Args:
        app_name: The name for the Spark application (default: "EMR Pipeline")
    
    Returns:
        SparkSession object
    """
    return SparkSession.builder.appName(app_name).getOrCreate()
