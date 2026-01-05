from common.spark_utils import get_spark_session
from pyspark.sql.functions import col, sum as _sum

def run():
    # Use local dummy data for testing
    input_path = "dataplatforms/data/dummy/sales/sales_data.csv"
    output_path = "dataplatforms/data/output/sales/"
    
    spark = get_spark_session()
    
    df = spark.read.option("header", True).csv(input_path)
    df.show()
    
    df_clean = df.withColumn("amount", col("amount").cast("float")).filter(col("amount").isNotNull())
    agg_df = (
        df_clean.groupBy("category", "order_date")
        .agg(_sum("amount").alias("total_revenue"))
    )
    
    agg_df.write.mode("overwrite").partitionBy("order_date").parquet(output_path)
    
    spark.stop()

run()