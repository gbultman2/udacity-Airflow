"""LoadFactOperator class."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """Class to load our data into the songplay fact table.

    :param redshift_conn_id
    :param sql_statement: sql statement that loads table
    :param table: the table to load
    :param sql_statement: the sql statement to insert
    :param create_table: allows creation or recreation of a table
    :param create_query: if create_table you need to specify this to
                            create the table
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='aws_redshift_connection',
                 table='',
                 sql_statement='',
                 create_table=False,
                 create_query='',
                 *args,
                 **kwargs):
        """init."""
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_statement = sql_statement
        self.create_table = create_table
        self.create_query = create_query

    def execute(self, context):
        """Execute Method."""
        self.log.info('Executing LoadFactOperator')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table:
            self.log.info(f"creating table {self.table}")
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
            redshift.run(self.create_query)
            self.log.info(f"Created table {self.table}")

        redshift.run(self.sql_statement)
        self.log.info(f"Completed loading for fact table {self.table}.")
