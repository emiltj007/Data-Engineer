from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id="",
                sql_statement="",
                table='', 
                append_mode=True,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info("Loading fact table")
        redshift_hook.run(str(self.sql_statement))
        
        
        
        self.log.info('LoadFactOperator implemented ')
