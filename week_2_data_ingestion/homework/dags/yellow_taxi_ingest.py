import os
import logging

from datetime import datetime
from tokenize import Triple

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
import pyarrow.csv as pv
import pyarrow.parquet as pq

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'dtc_week2_hw')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

EXECUTION_DATE_YEARMON = "{{ execution_date.strftime('%Y-%m') }}"
BQ_EXECUTION_DATE_YEARMON = EXECUTION_DATE_YEARMON.replace('-', '_')

TRIP_TYPE = 'yellow_taxi'
URL_FNAME_PREFIX = 'yellow_tripdata'

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_DATA = f"{URL_PREFIX}/{URL_FNAME_PREFIX}_{EXECUTION_DATE_YEARMON}.parquet"
OUTPUT_FNAME = f"output_{TRIP_TYPE}_{EXECUTION_DATE_YEARMON}.parquet"
OUTPUT_FPATH = f"{AIRFLOW_HOME}/{OUTPUT_FNAME}"
BUCKET_FPATH = f"raw/{TRIP_TYPE}/{OUTPUT_FNAME}"
BQ_TABLE_NAME = f"{TRIP_TYPE}_{BQ_EXECUTION_DATE_YEARMON}"

# Dag tasks functions
# NOTE: takes 20 mins, at an upload speed of 800kbps. Faster if your internet has a better upload speed
def upload_to_gcs(bucket, local_file_path, bucket_file_path):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    # End of Workaround

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(bucket_file_path)
    blob.upload_from_filename(local_file_path)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id=f'{TRIP_TYPE}_ingest',
    max_active_runs=3,
    schedule_interval="0 6 2 * *",
    start_date=datetime(2019, 1, 1),
    end_date=datetime(2020, 12, 3),
    default_args=default_args,
    tags=['dtc-de'],
) as data_dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_DATA} > {OUTPUT_FPATH}"
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "local_file_path": OUTPUT_FPATH,
            "bucket_file_path": BUCKET_FPATH,
        },
    )

    delete_local_dataset_task = BashOperator(
        task_id="delete_local_dataset",
        bash_command=f"rm {OUTPUT_FPATH}"
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": BQ_TABLE_NAME,
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/{BUCKET_FPATH}"],
            },
        },
    )

    download_dataset_task >> local_to_gcs_task >> delete_local_dataset_task >> bigquery_external_table_task
