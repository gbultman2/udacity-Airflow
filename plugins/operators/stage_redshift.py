"""StageToRedshiftOperator Class."""

from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """This operator will load JSON files from S3 to Redshift using copy.

    :param redshift_conn_id: the conn id to connect to redshift
                                (set up in airflow)
    :param aws_credentials_id: Aws credentials for s3 (set up in airflow)
    :param table: name of the target table
    :param s3_bucket: the name of the bucket hint(use variables in airflow)
    :param s3_key: the name of the key prefix (use variables in airflow)
    :param json_path: location of pathfile for JSON structure
    :param region: aws region
    :param create_table: whether you want to create/recreate the table
    :param create_query: if create_table then supply the table creation query
    """

    ui_color = '#358140'

    template_fields = ("s3_key",)

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='aws_redshift_connection',
                 aws_credentials_id='aws_credentials',
                 table='staging_events',
                 s3_bucket='udacity-airflow-gb',
                 s3_key='log-data/2018/11/2018-11-01-events.json',
                 json_path='log_json_path.json',
                 region='us-east-1',
                 create_table=False,
                 create_query='',
                 *args,
                 **kwargs):
        """Init."""
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.region = region
        self.create_table = create_table,
        self.create_query = create_query

    def execute(self, context):
        """Execute Method."""
        aws_hook = AwsHook(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        credentials = aws_hook.get_credentials()

        if self.create_table:
            self.log.info(f"Creating table {self.table}")
            redshift.run(f"DROP TABLE IF EXISTS {self.table}")
            redshift.run(self.create_query)

        self.log.info("Clearing data from destination Redshift table")
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        self.log.info(f"Rendered S3 Key: {rendered_key}")
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}/"

        if self.json_path == "auto":
            s3_json_path = "auto"
        else:
            s3_json_path = f"s3://{self.s3_bucket}/{self.json_path}"

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            s3_json_path,
            self.region
        )
        redshift.run(formatted_sql)
