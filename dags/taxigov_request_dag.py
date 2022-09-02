from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.models import Variable

import sql_statements

import datetime
import logging

import requests
from zipfile import ZipFile
from io import BytesIO

taxigov_s3_bucket = Variable.get("taxigov_s3_bucket")
taxigov_csv_file = Variable.get("taxigov_csv_s3_key")

default_args = {
    "owner": "udacity",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": 300,
    "catchup": False,
    "email_on_retry": False
}

def request_taxi_rides_data():
    r = requests.get("http://repositorio.dados.gov.br/seges/taxigov/taxigov-corridas-completo.zip")
    logging.info(f"Request result: {r.status_code}")

    with ZipFile(BytesIO(r.content)) as f:
        data = f.read("taxigov-corridas-completo.csv")

    return data

dag = DAG(
    "request_and_store_raw_taxigov_data",
    default_args=default_args,
    description="Request taxigov data and store it in S3",
    max_active_runs=1,
    schedule_interval="@monthly"
)

create_object_task = S3CreateObjectOperator(
    task_id="create_object",
    aws_conn_id="aws_credentials",
    s3_bucket=taxigov_s3_bucket,
    s3_key=taxigov_csv_file,
    data=request_taxi_rides_data(),
    replace=True,
    dag=dag
)

create_taxigov_raw_table_task = PostgresOperator(
    task_id="create_taxigov_raw_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_RAW__TAXIGOV_CORRIDAS_TABLE
)

transfer_s3_to_redshift_task = S3ToRedshiftOperator(
    task_id='transfer_s3_to_redshift',
    redshift_conn_id="redshift_default",
    aws_conn_id="aws_credentials",
    s3_bucket="brazilian-politicians-taxi-rides",
    s3_key=taxigov_csv_file,
    schema="PUBLIC",
    table="raw__taxigov_corridas",
    copy_options=["csv", "IGNOREHEADER 1", "MAXERROR 100"],
    dag=dag
)

create_object_task >> create_taxigov_raw_table_task >> transfer_s3_to_redshift_task

