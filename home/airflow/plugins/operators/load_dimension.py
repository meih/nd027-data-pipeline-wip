from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    truncate_sql = """
        TRUNCATE {}
    """

    insert_sql = """
        INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.select_sql = select_sql
        self.truncate_sql = truncate_sql
        self.insert_sql = insert_sql

    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Create a dimension table if not exists")
        redshift.run(create_sql)
#        formatted_truncate_sql = LoadDimensionOperator.truncate_sql.format(self.table)

        self.log.info("Insert into the dimension table")
        formatted_insert_sql = self.insert_sql.format(
            self.table,
            self.select_sql
        )
        redshift.run(formatted_insert_sql)
        
