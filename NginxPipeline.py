from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator

# Define the DAG
dag = DAG(
    'hello_world',
    description='A simple hello world DAG',
    schedule_interval=None,  # Daily at noon
    start_date=datetime(2024, 1, 1),
    catchup=False
)

CreateBucket = GCSCreateBucketOperator(
    task_id="CreateNewBucket",
    bucket_name="test-bucket",
    storage_class="STANDARD",
    location="US",
    labels={"env": "dev", "team": "airflow"},
    gcp_conn_id="airflow-conn-id",
    dag=dag
)


# If you have more tasks, define them here and set dependencies
# For this simple example, we only have one task
