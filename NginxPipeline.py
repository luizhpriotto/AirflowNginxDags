from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

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
    bucket_name="nginx-process-priotto",
    storage_class="STANDARD",
    location="US",
    labels={"env": "dev", "team": "airflow"},
    gcp_conn_id="airflow-conn-id",
    dag=dag
)

copy_files_with_match_glob = GCSToGCSOperator(
    task_id="copy_files_with_match_glob",
    source_bucket=estudos-gcp-priotto,
    source_object="nginx/",
    destination_bucket=nginx-process-priotto,
    destination_object="/",
    match_glob="**/*.csv",
)





CreateBucket >> copy_files_with_match_glob
