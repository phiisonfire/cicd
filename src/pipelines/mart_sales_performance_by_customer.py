from pathlib import Path
import sys
import os

# Get the directory 2 levels up
project_root = Path(__file__).resolve().parents[2]
print(project_root)

# Add it to sys.path
sys.path.insert(0, str(project_root))

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from src.extract import load_config, read_sql_table

def transform_sales_performance_by_customer(
    orders: DataFrame,
    order_items: DataFrame
) -> DataFrame:
    """Transforms and calculates sales performance by customer."""

    orders_with_order_items = orders \
        .join(order_items, orders.order_id == order_items.order_item_order_id, "inner")


    sales_by_customer = orders_with_order_items \
        .groupBy("order_customer_id") \
        .agg(
            F.sum("order_item_subtotal").alias("total_sales"),
            F.countDistinct("order_id").alias("number_of_orders")
        )
    
    return sales_by_customer

ENV = os.getenv("ENV", "development")

# Load configuration
config = load_config(ENV)

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Sales Performance By Customer") \
    .master(config["spark_master"]) \
    .config("spark.jars", config["spark_jars"]) \
    .getOrCreate()

orders_columns_to_select = ["order_id", "order_customer_id"]
orders = read_sql_table(
    spark, 
    config, 
    "orders", 
    "order_id",
    orders_columns_to_select
)

order_items_columns_to_select = ["order_item_order_id", "order_item_subtotal"]
order_items = read_sql_table(
    spark, 
    config, 
    "order_items", 
    "order_item_order_id",
    order_items_columns_to_select
)

sales_by_customer = transform_sales_performance_by_customer(orders, order_items)

sales_by_customer.show(10)