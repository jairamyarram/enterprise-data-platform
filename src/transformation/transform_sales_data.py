from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
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


def read_raw_data(spark, input_path, logger):
    logger.info(f"Reading raw data from {input_path}")
    return spark.read.parquet(input_path)


def remove_duplicates(df, logger):
    logger.info("Removing duplicate records")

    window_spec = Window.partitionBy("order_id").orderBy(col("order_date").desc())

    return (
        df.withColumn("row_num", row_number().over(window_spec))
          .filter(col("row_num") == 1)
          .drop("row_num")
    )


def apply_business_rules(df, logger):
    logger.info("Applying business rules")

    df = df.filter(col("amount") > 0)

    return df


def write_processed_data(df, output_path, logger):
    logger.info(f"Writing processed data to {output_path}")
    df.write.mode("overwrite").parquet(output_path)
    logger.info("Write completed")


def main():
    env = "dev"
    logger = get_logger("Transformation")

    config = load_config(env)

    spark = create_spark_session(config["spark"]["app_name"])

    input_path = "data/raw/sales"
    output_path = "data/processed/sales"

    df = read_raw_data(spark, input_path, logger)
    df = remove_duplicates(df, logger)
    df = apply_business_rules(df, logger)
    write_processed_data(df, output_path, logger)

    spark.stop()
    logger.info("Transformation job completed")


if __name__ == "__main__":
    main()
