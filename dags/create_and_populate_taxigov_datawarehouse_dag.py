from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.hooks.postgres_hook import PostgresHook

import sql_statements

import datetime
import logging

def check__dependencies(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Upstream Data Quality check failed. The request for {table} data returned no resuilts.")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Upstream Data Quality check failed. {table} contained 0 rows")
    logging.info(f"Data Quality for table {table} checks passed.")

dag = DAG(
    "create_and_populate_taxigov_datawarehouse",
    start_date=datetime.datetime.now(),
    schedule_interval="@monthly"
)

