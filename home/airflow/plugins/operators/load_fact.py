from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    
    ui_color = '#F98866'

    insert_sql = """
        INSERT INTO {} {}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 create_sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.select_sql = select_sql

    def execute(self, context):
        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create a fact table {self.table} if not exists")
        redshift.run(self.create_sql)

        self.log.info(f"Insert into the fact table {self.table}")
        formatted_insert_sql = self.insert_sql.format(
            self.table,
            self.select_sql
        )
        redshift.run(formatted_insert_sql)
