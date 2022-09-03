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
    "create_and_populate_public_expenses_datawarehouse",
    default_args=default_args,
    description="Create public expenses datawarehouse tables",
    max_active_runs=1,
    schedule_interval="@monthly"
)

check_raw_public_expenses_task = DataQualityOperator(
    task_id="check_raw_public_expenses_data",
    dag=dag,
    postgres_conn_id="redshift_default",
    table="raw__public_expenses_data"
)

create_dim_cost_centers_table_task = PostgresOperator(
    task_id="create_dim_cost_centers_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_COST_CENTERS
)

create_dim_cost_centers_relationship_table_task = PostgresOperator(
    task_id="create_dim_cost_centers_relationship_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_COST_CENTERS_RELATIONSHIP
)

create_dim_expenses_table_task = PostgresOperator(
    task_id="create_dim_expenses_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_EXPENSES
)

create_fact_monthly_expenses_table_task = PostgresOperator(
    task_id="create_fact_monthly_expenses_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_FACT_MONTHLY_EXPENSES
)

check_raw_public_expenses_task >> [create_dim_cost_centers_table_task, create_dim_expenses_table_task, create_dim_cost_centers_relationship_table_task] >> create_fact_monthly_expenses_table_task

