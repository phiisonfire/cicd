import os

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

def read_sql_table(
    spark: SparkSession,
    db_host: str = "localhost",
    jdbc_url: str = "jdbc:postgresql://localhost:5432/itversity_retail_db",
    table_name: str = "orders",
    username: str = "itversity_retail_user",
    password: str = "itversity"
) -> DataFrame:
    df = spark.read \
        .format("jdbc") \
        .option("url", jdbc_url) \
        .option("dbtable", table_name) \
        .option("user", username) \
        .option("password", password) \
        .option("driver", "org.postgresql.Driver") \
        .option("partitionColumn", "order_id") \
        .option("lowerBound", "1") \
        .option("upperBound", "10000") \
        .option("numPartitions", "8") \
        .load()
    return df
    

if __name__ == "__main__":
    spark = SparkSession.builder \
                .appName("Extract") \
                .master("local[4]") \
                .config("spark.jars", "jars/postgresql-42.7.4.jar") \
                .getOrCreate()
    df = read_sql_table(spark)
    df.show(10)
    spark.stop()
    