from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql_statement="",
                append_mode="False",
                table="",
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.table = table

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Truncate and load into dimensional tables")
        redshift_hook.run("DELETE FROM {}".format(self.table))
        redshift_hook.run(str(self.sql_statement))

        
        
        
        self.log.info('LoadDimensionOperator implemented ')
