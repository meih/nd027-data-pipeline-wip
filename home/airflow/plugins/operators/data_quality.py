from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 checks = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks

    def execute(self, context):
        redshift_hook = PostgresHook("redshift")

        for check in self.checks:
            self.log.info(f"Data quality check started.") 
            test_sql = check['test_sql']
            expected_result = check['check.expected_result']
            comparison = check['comparison']
            result = redshift_hook.get_first_record(test_sql)

            if comparison == '=':
                if result != expected_result:
                    raise AssertionError(f"Check failed: {result} {comparison} {expected_result}")
            if comparison == '!=':
                if result == expected_result:
                    raise AssertionError(f"Check failed: {result} {comparison} {expected_result}")
            if comparison == '>':
                if result <= expected_result:
                    raise AssertionError(f"Check failed: {result} {comparison} {expected_result}")
            if comparison == '<':
                if result <= expected_result:
                    raise AssertionError(f"Check failed: {result} {comparison} {expected_result}")
