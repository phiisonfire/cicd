from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "phinguyen",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="spark_mart_sales_performance_dag",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["spark"],
) as dag:

    run_mart_sales_performance = SparkSubmitOperator(
        task_id="run_mart_sales_performance",
        application="src/pipelines/mart_sales_performance_by_customer.py",
        conn_id="spark://spark-master:7077",  # Define this connection in Airflow
        jars="/path/to/jars/postgresql-42.7.4.jar",
        application_args=["--env", "production"],  # Example argument
    )