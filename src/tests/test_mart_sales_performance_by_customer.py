import sys
import os
from pathlib import Path

# Get the directory 2 levels up
project_root = Path(__file__).resolve().parents[2]
print(project_root)
# Add it to sys.path
sys.path.insert(0, str(project_root))

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row

from src.pipelines.mart_sales_performance_by_customer import transform_sales_performance_by_customer

def test_mart_sales_performance_by_customer():
    spark = SparkSession.builder \
        .appName("test-mart_sales_performance_by_customer") \
        .master("local[*]") \
        .getOrCreate()
    
    test_orders = spark.createDataFrame(
        [
            (1, 101),
            (2, 101),
            (3, 102),
            (4, 103)
        ],
        ["order_id", "order_customer_id"]
    )

    test_order_items = spark.createDataFrame(
        [
            (1, 10.0),
            (1, 15.0),
            (2, 20.0),
            (3, 30.0),
            (4, 100.0),
            (4, 1000.0)
        ],
        ["order_item_order_id", "order_item_subtotal"]
    )

    # apply transformation
    sales_by_customer_result = transform_sales_performance_by_customer(test_orders, test_order_items)

    # Check if the output DataFrame has the correct schema
    expected_schema = ["order_customer_id", "total_sales", "number_of_orders"]
    assert sales_by_customer_result.columns == expected_schema, "Schema mismatch in the result DataFrame"

    # Convert the DataFrame to a list of Rows for easier validation
    result_data = sales_by_customer_result.collect()

    # Check if the correct number of records are returned
    assert len(result_data) == 3, "Expected 3 customer records, but got a different count"

    # Validate specific customer results
    expected_data = {
        101: {"total_sales": 45.0, "number_of_orders": 2},
        102: {"total_sales": 30.0, "number_of_orders": 1},
        103: {"total_sales": 1100.0, "number_of_orders": 1},
    }

    for row in result_data:
        customer_id = row["order_customer_id"]
        assert customer_id in expected_data, f"Unexpected customer ID: {customer_id}"
        assert row["total_sales"] == expected_data[customer_id]["total_sales"], (
            f"Mismatch in total_sales for customer {customer_id}: "
            f"expected {expected_data[customer_id]['total_sales']}, got {row['total_sales']}"
        )
        assert row["number_of_orders"] == expected_data[customer_id]["number_of_orders"], (
            f"Mismatch in number_of_orders for customer {customer_id}: "
            f"expected {expected_data[customer_id]['number_of_orders']}, got {row['number_of_orders']}"
        )

    # Stop SparkSession after the test
    spark.stop()


