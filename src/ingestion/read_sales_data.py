from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import yaml

from src.utils.logger import get_logger


def load_config(env: str):
    with open(f"configs/{env}.yaml", "r") as file:
        return yaml.safe_load(file)


def create_spark_session(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .getOrCreate()
    )


def read_sales_data(spark, input_path, logger):
    logger.info(f"Reading data from {input_path}")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(input_path)
    )

    logger.info(f"Rows read: {df.count()}")
    return df


def validate_data(df, logger):
    logger.info("Validating data")

    if "order_id" not in df.columns:
        raise Exception("order_id column missing")

    if df.filter(col("order_id").isNull()).count() > 0:
        raise Exception("order_id has null values")

    logger.info("Validation passed")


def write_raw_data(df, output_path, logger):
    logger.info(f"Writing raw data to {output_path}")

    df.write.mode("overwrite").parquet(output_path)

    logger.info("Write completed")


def main():
    env = "dev"
    logger = get_logger("Ingestion")

    config = load_config(env)

    spark = create_spark_session(config["spark"]["app_name"])

    input_path = "data/sample/sales_data.csv"
    output_path = "data/raw/sales"

    df = read_sales_data(spark, input_path, logger)
    validate_data(df, logger)
    write_raw_data(df, output_path, logger)

    spark.stop()
    logger.info("Job finished successfully")


if __name__ == "__main__":
    main()
