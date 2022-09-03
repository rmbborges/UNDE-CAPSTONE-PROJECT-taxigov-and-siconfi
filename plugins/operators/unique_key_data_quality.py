import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class UniqueKeyCheckOperator(BaseOperator):

    @apply_defaults
    def __init__(self,
                 postgres_conn_id="",
                 table="",
                 column="",
                 *args, **kwargs):

        super(UniqueKeyCheckOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.postgres_conn_id = postgres_conn_id
        self.column = column

    def execute(self, context):
        redshift_hook = PostgresHook(self.postgres_conn_id)
        records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {self.table}")
        unique_key_records = redshift_hook.get_records(f"SELECT COUNT(DISTINCT({self.column})) FROM {self.table}")

        num_records = records[0][0]
        num_unique_key_records = unique_key_records[0][0]

        if num_records != num_unique_key_records:
            raise ValueError(f"Data quality check failed. {self.column} has non-unique records")
        logging.info(f"Data quality on table {self.table} check passed")
