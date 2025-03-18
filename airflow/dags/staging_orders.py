from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime


INGESTION_ENTITY = "orders"

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 17),
    'retries': 0,
}

with DAG(
    f'staging_{INGESTION_ENTITY}',
    default_args=default_args,
    description='A simple Spark submit DAG',
    schedule_interval=None,
) as dag:

    # Define the SparkSubmitOperator
    spark_submit = SparkSubmitOperator(
        task_id='spark_submit_task',
        conn_id='spark_default',  # Ensure you have the correct connection ID
        application='/opt/airflow/dags/src/spark/ingestion.py',  # Path to your Spark app
        packages="com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.28.0",
        name=f'ingest_{INGESTION_ENTITY}_data',
        verbose=True,
        application_args=[
            "--input_url", f"{Variable.get("JAFFLE_DATA_BASE_URL")}_{INGESTION_ENTITY}.csv",
            "--bq_project", "lively-hall-447909-i7",
            "--bq_dataset", "staging",
            "--bq_table", INGESTION_ENTITY,
            "--gcp_keyfile", Variable.get("GCP_KEYFILE_PATH")
        ]
    )

    spark_submit
