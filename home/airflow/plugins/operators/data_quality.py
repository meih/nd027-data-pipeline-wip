from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        for table in self.tables:
            self.log.info(f"Data quality check start with {table}.") 
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {table}")

            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed: {table} returned no results")

            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed: {table} contained 0 rows")

            self.log.info(f"Data quality check passed: {table} has {records[0][0]} records")

        