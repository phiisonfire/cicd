import os
import yaml
from pyspark.sql import SparkSession, DataFrame


def load_config(env: str, config_file: str = "conf/dev.yaml") -> dict:
    with open(config_file, "r") as file:
        config = yaml.safe_load(file)
    return config["environments"][env]


def read_sql_table(
        spark: SparkSession, 
        config: dict,
        table: str = "orders",
        primary_key: str = "order_id",
        columns: list = None  # Add a parameter for selecting specific columns
) -> DataFrame:
    # Build the SQL query to select specific columns if provided
    if columns:
        column_list = ", ".join(columns)
        table_query = f"(SELECT {column_list} FROM {table}) AS subquery"
    else:
        table_query = table  # Use the entire table if no columns are specified

    df = spark.read \
        .format("jdbc") \
        .option("url", config["jdbc_url"]) \
        .option("dbtable", table_query) \
        .option("user", config["username"]) \
        .option("password", config["password"]) \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", primary_key) \
        .option("lowerBound", config["lower_bound"]) \
        .option("upperBound", config["upper_bound"]) \
        .option("numPartitions", config["num_partitions"]) \
        .load()
    return df


if __name__ == "__main__":
    # Set environment (e.g., development or test)
    ENV = os.getenv("ENV", "development")

    # Load configuration
    config = load_config(ENV)

    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Extract") \
        .master(config["spark_master"]) \
        .config("spark.jars", config["spark_jars"]) \
        .getOrCreate()

    # Read data
    df = read_sql_table(spark, config)
    df.show(10)

    # Stop Spark session
    spark.stop()