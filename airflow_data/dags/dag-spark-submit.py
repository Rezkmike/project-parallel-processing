from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
}

with DAG('spark_parallel_processing',
         default_args=default_args,
         schedule_interval=None) as dag:

    spark_task = SparkSubmitOperator(
        task_id='spark_job',
        application='/opt/spark-apps/spark_app.py',  # Path to your Spark application
        conn_id='spark_default',  # Predefined Spark connection
        verbose=True,
    )
