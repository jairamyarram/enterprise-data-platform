from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as sum_, count, month, year
import yaml

from src.utils.logger import get_logger


def load_config(env):
    with open(f"configs/{env}.yaml", "r") as file:
        return yaml.safe_load(file)


def create_spark_session(app_name):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def read_processed_data(spark, input_path, logger):
    logger.info(f"Reading processed data from {input_path}")
    return spark.read.parquet(input_path)


def aggregate_sales(df, logger):
    logger.info("Aggregating sales data")

    # Example 1: Total sales per customer
    customer_sales = df.groupBy("customer_id").agg(
        sum_("amount").alias("total_amount"),
        count("order_id").alias("total_orders")
    )

    # Example 2: Monthly sales
    monthly_sales = df.withColumn("year", year(col("order_date"))) \
                      .withColumn("month", month(col("order_date"))) \
                      .groupBy("year", "month") \
                      .agg(
                          sum_("amount").alias("monthly_total"),
                          count("order_id").alias("monthly_orders")
                      )

    return customer_sales, monthly_sales


def write_analytics_data(customer_df, monthly_df, output_base, logger):
    logger.info(f"Writing analytics data to {output_base}/customer")
    customer_df.write.mode("overwrite").parquet(f"{output_base}/customer")

    logger.info(f"Writing analytics data to {output_base}/monthly")
    monthly_df.write.mode("overwrite").parquet(f"{output_base}/monthly")

    logger.info("Analytics data write completed")


def main():
    env = "dev"
    logger = get_logger("Analytics")

    config = load_config(env)
    spark = create_spark_session(config["spark"]["app_name"])

    input_path = "data/processed/sales"
    output_path = "data/analytics/sales"

    df = read_processed_data(spark, input_path, logger)
    customer_df, monthly_df = aggregate_sales(df, logger)
    write_analytics_data(customer_df, monthly_df, output_path, logger)

    spark.stop()
    logger.info("Analytics job completed successfully")


if __name__ == "__main__":
    main()
