# import all modules
import airflow
from airflow import DAG
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator,
)

# Variables to be used in the DAG
PROJECT_ID = "linen-age-447106-e3"
REGION = "asia-south1"
CLUSTER_NAME = "dataproc-cluster"
GCS_JOB_FILE = "gs://project-bucket-for-pipeline/real_estate_data/real_estate_processing.py"
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 50},
    },
    "worker_config": {
        "num_instances": 3,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-balanced", "boot_disk_size_gb": 50},
    }
}

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE},
}

# Define the default arguments for the DAG
args = {
    "owner": "Rahul",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=5),
}



# Define the DAG 
with DAG(
    dag_id="dataproc_job",
    schedule_interval="30 8 * * *",
    description="A DAG to create the dataproc cluster, run the pyspark job, and delete the cluster",
    default_args=args,
    tags=["dataproc", "pyspark", "real_estate", "etl"],
) as dag :
    
    # Task 1 : Create a Dataproc cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME
    )

    # Task 2 : Submit the pyspark job to the cluster
    pyspark_task = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job", 
        job=PYSPARK_JOB, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    # Task 3 : Delete the Dataproc cluster 
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
    )

# Define the task dependencies
create_cluster >> pyspark_task >> delete_cluster