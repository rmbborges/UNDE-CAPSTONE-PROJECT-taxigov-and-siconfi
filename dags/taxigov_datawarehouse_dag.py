from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import DataQualityOperator

import sql_statements

import datetime
import logging

default_args = {
    "owner": "udacity",
    "start_date": datetime.datetime.now(),
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": 60,
    "catchup": False,
    "email_on_retry": False
}

dag = DAG(
    "create_and_populate_taxigov_datawarehouse",
    default_args=default_args,
    description="Create the taxigov datawarehouse tables",
    max_active_runs=1,
    schedule_interval="@monthly"
)

check_raw_taxigov_task = DataQualityOperator(
    task_id="check_raw_taxigov_data",
    dag=dag,
    postgres_conn_id="redshift_default",
    table="raw__taxigov_corridas"
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

check_raw_taxigov_task >> [create_dim_requests_table_task, create_dim_rides_table_task, create_dim_dates_table_task] >> create_fact_daily_rides_table_task

