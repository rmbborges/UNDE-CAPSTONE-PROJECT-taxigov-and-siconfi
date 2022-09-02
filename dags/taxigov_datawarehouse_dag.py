from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

import sql_statements

import datetime
import logging

default_args = {
    "owner": "udacity",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": 300,
    "catchup": False,
    "email_on_retry": False
}

def check_dependencies(*args, **kwargs):
    table = kwargs["params"]["table"]
    redshift_hook = PostgresHook("redshift_default")
    records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")
    if len(records) < 1 or len(records[0]) < 1:
        raise ValueError(f"Upstream Data Quality check failed. The request for {table} data returned no resuilts.")
    num_records = records[0][0]
    if num_records < 1:
        raise ValueError(f"Upstream Data Quality check failed. {table} contained 0 rows")
    logging.info(f"Data Quality for table {table} checks passed.")

dag = DAG(
    "create_and_populate_taxigov_datawarehouse",
    default_args=default_args,
    description="Create the taxigov datawarehouse tables",
    max_active_runs=1,
    schedule_interval="@monthly"
)

check_raw_taxigov_quality_task = PythonOperator(
    task_id="check_taxigov_data",
    dag=dag,
    python_callable=check_dependencies,
    provide_context=True,
    params={
        "table": "raw__taxigov_corridas",
    }
)

create_dim_requests_table_task = PostgresOperator(
    task_id="create_dim_requests_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_REQUESTS_TABLE
)

create_dim_rides_table_task = PostgresOperator(
    task_id="create_dim_rides_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_RIDES_TABLE
)

create_dim_dates_table_task = PostgresOperator(
    task_id="create_dim_dates_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_DATES_TABLE
)

create_fact_daily_rides_table_task = PostgresOperator(
    task_id="create_fact_daily_rides_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_FACT_DAILY_RIDES_TABLE
)

check_raw_taxigov_quality_task >> [create_dim_requests_table_task, create_dim_rides_table_task, create_dim_dates_table_task] >> create_fact_daily_rides_table_task
