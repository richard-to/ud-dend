import logging

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Operator to stage S3 data into Redshift for further processing

    To load data incrementally based on execution time and backfill runs
    you can pass a function into `s3_bucket_path` instead of a string.

    The operator's context will be passed into the callback fucntion

    Example using context:
        lambda ctx: "s3://udacity-dend/log_data/{}/{}/{}-events.json".format(
            ctx["ds"][:4],
            ctx["ds"][5:7],
            ctx["ds"],
       )

    Attributes:
        aws_credentials_id: Name of Airflow connection for AWS user
        redshift_conn_id: Name of Airflow connection for Redshift
        s3_bucket_path: S3 bucket path where staging data resides or a function that creates the path
        target_table: Redshift table to insert staging data
        json_formatter: JSON formatter option for Redshift COPY command (defaults to 'auto ignorecase')
    """
    ui_color = "#358140"

    stage_sql_template = """
    COPY {target_table} from '{s3_bucket_path}'
    CREDENTIALS 'aws_access_key_id={access_key};aws_secret_access_key={secret_key}'
    REGION 'us-west-2'
    FORMAT AS JSON '{json_formatter}';
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 redshift_conn_id="",
                 s3_bucket_path="",
                 s3_path_prefix="",
                 target_table="",
                 json_formatter="auto ignorecase",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.json_formatter = json_formatter
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket_path = s3_bucket_path
        self.target_table = target_table

    def execute(self, context):
        if callable(self.s3_bucket_path):
            # If s3_bucket_path is a function, call it to dynamically generate
            # a path based on the given context.
            s3_bucket_path = self.s3_bucket_path(context)
        else:
            s3_bucket_path = self.s3_bucket_path

        logging.info(f"Loading data from {self.s3_bucket_path} to {self.target_table} table.")

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(self.stage_sql_template.format(
            s3_bucket_path=s3_bucket_path,
            target_table=self.target_table,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            json_formatter=self.json_formatter,
        ))
