from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

from airflow.models import Variable

import sql_statements

import datetime
import logging

import requests
import json

expenses_s3_bucket = Variable.get("expenses_s3_bucket")

default_args = {
    "owner": "udacity",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": 60,
    "catchup": False,
    "email_on_retry": False
}

def request_iterator(year, month):
    session = requests.Session()
    print("Starting Request")
    url = f"https://apidatalake.tesouro.gov.br/ords/custos/tt/demais?organizacao=235876&ano={str(year)}&mes={str(month)}"
    first_page = session.get(url)
    print("Status code:", first_page.status_code)
    if first_page.status_code == 200:
        first_page_content = first_page.json()
        limit = first_page_content["limit"]
        yield first_page_content
        
        next_page_content = first_page_content
        while next_page_content["hasMore"] == True:
            limit += next_page_content["count"]
            next_page_url = url + "&offset=" + str(limit)
            next_page = session.get(next_page_url)
            next_page_content = next_page.json()
            
            if next_page.status_code != 200:
                break
            yield next_page_content

def request_public_expenses_data(year, month):
    s3_hook = S3Hook(aws_conn_id="aws_credentials")
    
    data = []
    logging.info(f"Starting request for url https://apidatalake.tesouro.gov.br/ords/custos/tt/demais?organizacao=235876&ano={str(year)}&mes={str(month)}")
    for page in request_iterator(year=year, month=month):
        logging.info(page)
        data += page["items"]

    dumped_string = json.dumps(data)
    final_dumped_string = dumped_string.replace("[", "").replace("]", "").replace("},", "}")

    s3_hook.load_string(
        string_data=final_dumped_string,
        key=f"data_{year}{month}.json",
        bucket_name=expenses_s3_bucket,
        replace=True
    )

dag = DAG(
    "request_and_store_raw_public_expenses_data",
    default_args=default_args,
    description="Requests siconfi expenses data, stores it in S3 and populate raw__public_expenses_data table in Redshift",
    max_active_runs=1,
    schedule_interval="@daily"
)

save_public_expenses_data_to_s3_task = PythonOperator(
    task_id="save_public_expenses_data_to_s3_task",
    python_callable=request_public_expenses_data,
    op_kwargs={
        "year": "{{ execution_date.year }}", 
        "month": "{{ execution_date.month }}"
    },
    dag=dag
)

create_public_expenses_raw_table_task = PostgresOperator(
    task_id="create_public_expenses_raw_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_RAW__PUBLIC_EXPENSES_DATA
)

transfer_s3_to_redshift_task = S3ToRedshiftOperator(
    task_id='transfer_s3_to_redshift',
    redshift_conn_id="redshift_default",
    aws_conn_id="aws_credentials",
    s3_bucket=expenses_s3_bucket,
    s3_key="data_{{ execution_date.year }}{{ execution_date.month }}.json",
    schema="PUBLIC",
    table="raw__public_expenses_data",
    copy_options=["format json 'auto'", "BLANKSASNULL", "MAXERROR 20"],
    method="UPSERT",
    upsert_keys=["co_situacao_icc", "co_natureza_despesa_deta", "me_referencia", "an_referencia", "co_natureza_despesa_deta"],
    dag=dag
)

save_public_expenses_data_to_s3_task >> create_public_expenses_raw_table_task >> transfer_s3_to_redshift_task