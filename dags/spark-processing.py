from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

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
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["spark", "example"],
) as dag:
    
    # Task: Submit Spark job
    spark_job = SparkSubmitOperator(
        task_id="spark_submit_job",
        application="/opt/spark-apps/sample_spark_app.py",  # Path to your Spark application
        conn_id="spark_default",  # Define in Airflow connections
        verbose=True,
        application_args=["/opt/spark-data/input", "/opt/spark-data/output"],  # Example arguments
    )
    
    spark_job
