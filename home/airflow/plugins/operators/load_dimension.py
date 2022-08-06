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
                 redshift_conn_id="",
                 table="",
                 select_sql="",
                 create_sql="",
                 trunc_flag=True,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.create_sql = create_sql
        self.select_sql = select_sql
        self.trunc_flag = trunc_flag

    def execute(self, context):
        self.log.info("Connecting to Redshift...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info(f"Create a dimension table {self.table} if not exists")
        redshift.run(self.create_sql)

        if self.trunc_flag:
            self.log.info(f"Truncating the dimension table {self.table}")
            formatted_truncate_sql = self.truncate_sql.format(self.table)
            redshift.run(formatted_truncate_sql)

        self.log.info(f"Insert into the dimension table {self.table}")
        formatted_insert_sql = self.insert_sql.format(
            self.table,
            self.select_sql
        )
        redshift.run(formatted_insert_sql)
        
