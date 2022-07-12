import os
from datetime import datetime

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from ingest_script import ingest_callable

PG_HOST = os.getenv('PG_HOST')
PG_USER = os.getenv('PG_USER')
PG_PASSWORD = os.getenv('PG_PASSWORD')
PG_PORT = os.getenv('PG_PORT')
PG_DATABASE = os.getenv('PG_DATABASE')

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

local_workflow = DAG(
    "LocalIngestionDag",
    schedule_interval="0 6 2 * *",
    start_date=datetime(2021, 1, 1)
)

EXECUTION_DATE_YEARMON = "{{ execution_date.strftime('%Y-%m') }}"
URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"
URL_TEMPLATE = f"{URL_PREFIX}/yellow_tripdata_{EXECUTION_DATE_YEARMON}.parquet"
OUTPUT_FILE_TEMPLATE = f"{AIRFLOW_HOME}/output_{EXECUTION_DATE_YEARMON}.parquet"
TABLE_NAME_TEMPLATE = "yellow_taxis_{{ execution_date.strftime('%Y_%m') }}"

with local_workflow:

    wget_task = BashOperator(
        task_id='wget',
        bash_command=f'wget {URL_TEMPLATE} -O {OUTPUT_FILE_TEMPLATE}'
    )

    ingest_task = PythonOperator(
        task_id='ingest',
        python_callable=ingest_callable,
        op_kwargs=dict(
            user=PG_USER,
            password=PG_PASSWORD,
            host=PG_HOST,
            port=PG_PORT,
            db=PG_DATABASE,
            table_name=TABLE_NAME_TEMPLATE,
            pq_f_name=OUTPUT_FILE_TEMPLATE,
        )
    )

    wget_task >> ingest_task

