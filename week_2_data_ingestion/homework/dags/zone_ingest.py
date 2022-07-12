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

# Parquet file giving me access denied
# URL_DATA = "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.parquet"
URL_DATA = 'https://raw.githubusercontent.com/alexeygrigorev/datasets/master/nyc-tlc/taxi%2B_zone_lookup.csv'
# OUTPUT_FPATH = f"{AIRFLOW_HOME}/zones.parquet"
OUTPUT_FPATH = f"{AIRFLOW_HOME}/zones.csv"
BUCKET_FPATH = "raw/zones.csv"
OUTPUT_FPATH_PQ = OUTPUT_FPATH.replace('.csv', '.parquet')
BUCKET_FPATH_PQ = BUCKET_FPATH.replace('.csv', '.parquet')
BQ_TABLE_NAME = "zones"

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

def format_to_parquet(src_file):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)
    pq.write_table(table, OUTPUT_FPATH_PQ)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
}

with DAG(
    dag_id=f'zones_ingest',
    max_active_runs=3,
    start_date=days_ago(1),
    schedule_interval='@once',
    default_args=default_args,
    tags=['dtc-de'],
) as data_dag:
    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSL {URL_DATA} > {OUTPUT_FPATH}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{OUTPUT_FPATH}",
        },
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "local_file_path": OUTPUT_FPATH_PQ,
            "bucket_file_path": BUCKET_FPATH_PQ,
        },
    )

    delete_local_dataset_task = BashOperator(
        task_id="delete_local_dataset",
        bash_command=f"rm {OUTPUT_FPATH} {OUTPUT_FPATH_PQ}"
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
                "sourceUris": [f"gs://{BUCKET}/{BUCKET_FPATH_PQ}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_local_dataset_task >> bigquery_external_table_task
