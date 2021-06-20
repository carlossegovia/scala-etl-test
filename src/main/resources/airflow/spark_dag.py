import os

from airflow import models
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
from airflow.utils.dates import days_ago

# GCP Parameters
PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project_id")
CLUSTER_NAME = os.environ.get("GCP_DATAPROC_CLUSTER_NAME=", "cluster-name")
REGION = os.environ.get("GCP_LOCATION", "southamerica-east1")
ZONE = os.environ.get("GCP_REGION", "southamerica-east1-a")
BUCKET = os.environ.get("GCP_DATAPROC_BUCKET", "bucket-name")

# Spark Parameters
OUTPUT_FOLDER = "applaudo_etl"
OUTPUT_PATH = f"gs://{BUCKET}/{OUTPUT_FOLDER}/"
MAIN_CLASS = "StartETL"

# Cluster definition
# [START how_to_cloud_dataproc_create_cluster]

CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-2",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-standard", "boot_disk_size_gb": 1024},
    },
}

# [END how_to_cloud_dataproc_create_cluster]

TIMEOUT = {"seconds": 1 * 24 * 60 * 60}

# [START how_to_cloud_dataproc_spark_config]

SPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "spark_job": {
        "main_jar_file_uri": f"gs://{BUCKET}/ApplaudoETL.jar",
        "main_class": MAIN_CLASS,
        "jar_file_uris": [f"gs://{BUCKET}/mssql-jdbc-9.2.1.jre8.jar"],
        "args": ["-r", OUTPUT_PATH]
    },
}

# [END how_to_cloud_dataproc_spark_config]


with models.DAG("example_gcp_dataproc", start_date=days_ago(1), schedule_interval=None) as dag:
    # [START how_to_cloud_dataproc_create_cluster_operator]
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )
    # [END how_to_cloud_dataproc_create_cluster_operator]

    # [START how_to_cloud_dataproc_submit_job_to_cluster_operator]
    spark_task = DataprocSubmitJobOperator(
        task_id="spark_task", job=SPARK_JOB, location=REGION, project_id=PROJECT_ID
    )
    # [END how_to_cloud_dataproc_submit_job_to_cluster_operator]

    # [START how_to_cloud_dataproc_delete_cluster_operator]
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster", project_id=PROJECT_ID, cluster_name=CLUSTER_NAME, region=REGION
    )
    # [END how_to_cloud_dataproc_delete_cluster_operator]

    create_cluster >> spark_task >> delete_cluster
