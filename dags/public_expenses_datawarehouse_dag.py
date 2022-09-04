from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

from operators.upstream_data_quality import (UpstreamDependencyCheckOperator)
from operators.unique_key_data_quality import (UniqueKeyCheckOperator)

import sql_statements

import datetime

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
    schedule_interval="@daily"
)

check_raw_public_expenses_task = UpstreamDependencyCheckOperator(
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

check_dim_cost_centers_unique_key = UniqueKeyCheckOperator(
    task_id="check_dim_cost_centers_unique_key",
    dag=dag,
    postgres_conn_id="redshift_default",
    table="dim_cost_centers",
    column="id"
)

create_dim_cost_centers_relationship_table_task = PostgresOperator(
    task_id="create_dim_cost_centers_relationship_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_COST_CENTERS_RELATIONSHIP
)

check_dim_cost_centers_relationship_unique_key = UniqueKeyCheckOperator(
    task_id="check_dim_cost_centers_relationship_unique_key",
    dag=dag,
    postgres_conn_id="redshift_default",
    table="dim_cost_centers_relationship",
    column="id"
)

create_dim_expenses_table_task = PostgresOperator(
    task_id="create_dim_expenses_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_DIM_EXPENSES
)

check_dim_expenses_unique_key = UniqueKeyCheckOperator(
    task_id="check_dim_expenses_unique_key",
    dag=dag,
    postgres_conn_id="redshift_default",
    table="dim_expenses",
    column="id"
)

create_fact_monthly_expenses_table_task = PostgresOperator(
    task_id="create_fact_monthly_expenses_table",
    dag=dag,
    postgres_conn_id="redshift_default",
    sql=sql_statements.CREATE_FACT_MONTHLY_EXPENSES
)

check_raw_public_expenses_task >> create_dim_cost_centers_table_task >> check_dim_cost_centers_unique_key
check_raw_public_expenses_task >> create_dim_expenses_table_task >> check_dim_expenses_unique_key
check_raw_public_expenses_task >> create_dim_cost_centers_relationship_table_task >> check_dim_cost_centers_relationship_unique_key
[check_dim_cost_centers_unique_key, check_dim_expenses_unique_key, check_dim_cost_centers_relationship_unique_key] >> create_fact_monthly_expenses_table_task

