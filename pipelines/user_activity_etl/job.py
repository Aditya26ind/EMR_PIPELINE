from pyspark.sql.functions import count
from common.spark_session import get_spark

def run():
    spark = get_spark("User_Activity_ETL")

    input_path = "s3://my-bucket/raw/user_activity/"
    output_path = "s3://my-bucket/processed/user_activity/"

    df = spark.read.option("header", True).json(input_path)

    agg_df = (
        df.groupBy("event_type", "event_date")
        .agg(count("*").alias("event_count"))
    )

    agg_df.write.mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(output_path)

    spark.stop()

if __name__ == "__main__":
    run()
