from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator,DataprocCreateClusterOperator,DataprocDeleteClusterOperator
from airflow.providers.google.cloud.sensors.dataproc import DataprocJobSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

# Define the DAG
dag = DAG(
    'FifaPlayers',
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
    source_bucket="estudos-gcp-priotto",
    source_object="nginx/",
    destination_bucket="nginx-process-priotto",
    destination_object="players/",
    match_glob="**/*.csv",
    gcp_conn_id="airflow-conn-id",
    dag=dag
)

CLUSTER_CONFIG = {
    "gce_cluster_config": {
       # "network_uri": "k8s",
        "subnetwork_uri": "k8s",
    },
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 32},
    },
}

create_cluster = DataprocCreateClusterOperator(
    task_id="create_cluster",
    project_id="estudos-gcp-433522",
    cluster_config=CLUSTER_CONFIG,
    region="us-central1",
    cluster_name="cluster-spark",
    use_if_exists=True,
    gcp_conn_id="airflow-conn-id",
    dag=dag
)

PYSPARK_JOB = {
    "reference": {"project_id": "estudos-gcp-433522"},
    "placement": {"cluster_name": "cluster-spark"},
    "pyspark_job": {"main_python_file_uri": "gs://estudos-gcp-priotto/spark.py"},
}

pyspark_task = DataprocSubmitJobOperator(
    task_id="pyspark_task", 
    job=PYSPARK_JOB,
    region="us-central1",
    project_id="estudos-gcp-433522",
    gcp_conn_id="airflow-conn-id",
    dag=dag
)

dataproc_job_sensor = DataprocJobSensor(
    task_id='dataproc_job_sensor',
    region='us-central1',
    project_id='estudos-gcp-433522',
    dataproc_job_id="{{ task_instance.xcom_pull(task_ids='pyspark_task') }}",
    poke_interval=30,  # Intervalo de 30 segundos entre verificações
    timeout=600,  # Tempo máximo de espera para o job ser concluído (10 minutos)
    gcp_conn_id="airflow-conn-id",
    dag=dag
)


delete_cluster = DataprocDeleteClusterOperator(
    task_id="delete_cluster",
    project_id="estudos-gcp-433522",
    cluster_name="cluster-spark",
    region="us-central1",
    gcp_conn_id="airflow-conn-id",
    dag=dag
)

create_new_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_new_dataset',
    dataset_id='fifa',
    project_id='estudos-gcp-433522',
    dataset_reference={"friendlyName": "New Dataset"},
    gcp_conn_id="airflow-conn-id",
    dag=dag
    )

load_csv = GCSToBigQueryOperator(
    task_id='load_csv',
    bucket='nginx-process-priotto',
    source_objects=['result/*.csv'],
    destination_project_dataset_table=f"{'fifa'}.{'players'}",
    skip_leading_rows=1,
    write_disposition='WRITE_TRUNCATE',
    gcp_conn_id="airflow-conn-id",
    dag=dag
)

CreateBucket >> copy_files_with_match_glob >> create_cluster >> pyspark_task >> dataproc_job_sensor >> create_new_dataset >> load_csv >> delete_cluster
