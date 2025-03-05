"""LoadDimensionOperator Class."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """Class for inserting data to the dimension tables.

    :param redshift_conn_id: the redshift connection id set up in airflow
    :param sql_statement: the sql statement to execute
    :param table: the target table for the dimension
    :param append: True appends False truncates table before insertion
    :param create_table: Allows you to recreate the table (drops table first)
    :praem create_query: if create_table, specify the table creation
    """

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='aws_redshift_connection',
                 sql_statement='',
                 table='',
                 append=True,
                 create_table=False,
                 create_query='',
                 *args,
                 **kwargs):
        """init."""
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_statement = sql_statement
        self.table = table
        self.append = append
        self.create_table = create_table
        self.create_query = create_query

    def execute(self, context):
        """Execute Method."""
        self.log.info('LoadDimension Running')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table:
            self.log.info(f"Creating table {self.table}")
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
            redshift.run(self.create_query)
            self.log.info(f'Created table {self.table}')

        if not self.append:
            self.log.info(f"Truncating table {self.table}")
            redshift.run(f"TRUNCATE TABLE {self.table}")

        self.log.info(f"Inserting data to {self.table}")
        redshift.run(self.sql_statement)
        self.log.info(f"Insertion Complete for {self.table}")
