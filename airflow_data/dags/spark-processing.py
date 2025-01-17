from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    "spark_processing_dag",
    default_args=default_args,
    description="DAG to process data using Spark",
    schedule_interval=None,  # Run manually or set as required
    start_date=days_ago(1),  # Use days_ago for easier debugging
    catchup=False,
    tags=["spark", "example"],
) as dag:

    # Task: Submit Spark job
    spark_job = SparkSubmitOperator(
        task_id="spark_submit_job",
        application="/opt/spark-apps/sample_spark_app.py",  # Path to your Spark application
        verbose=True,  # Ensure verbose output
        application_args=["/opt/spark-data/input", "/opt/spark-data/output"],  # Example arguments
        conf={
            "spark.submit.deployMode": "client",
            "spark.master": "spark://spark-master:7077"
        },
        executor_cores=2,
        executor_memory="2g",
        driver_memory="2g"
    )

    spark_job
