"""data_quality.py."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Class that performs data quality checks.

    :param sql_queries: list of SQL queries to run for data quality checks
    :param expected_results: list of expected results for each SQL query
    :param redshift_conn_id: redshift connection (set up in Airflow)
    :param table: table name for the data quality checks
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 sql_queries=None,
                 expected_results=None,
                 redshift_conn_id='aws_redshift_connection',
                 *args,
                 **kwargs):
        """Class."""
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.sql_queries = sql_queries
        self.expected_results = expected_results
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """Execute method."""
        self.log.info("Starting Data Quality checks")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for i, query in enumerate(self.sql_queries):

            self.log.info(f"Running query: {query}")

            result = redshift.get_first(query)

            if result is None:
                raise ValueError(f"Data Quality check failed. No data \
                                 returned for query: {query}")

            actual_result = result[0]

            if actual_result != self.expected_results[i]:
                raise ValueError(f"Data Quality check failed for query: \
                                 {query}. Expected \
                                     {self.expected_results[i]}, \
                                         but got {actual_result}.")
            else:
                self.log.info(f"Data Quality check passed for query: \
                              {query}. Expected: \
                                  {self.expected_results[i]}, \
                                      Got: {actual_result}.")
