import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    """Operator to check data quality

    Attributes:
        data_checks: List of DataCheck objects
        redshift_conn_id: Name of Airflow connection for Redshift
    """

    ui_color = "#89DA59"

    @apply_defaults
    def __init__(self,
                 data_checks=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.data_checks = data_checks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i, data_check in enumerate(self.data_checks):
            logging.info(f"[running] Data check {i}: {data_check.name}")
            records = redshift.get_records(data_check.sql_query)
            if data_check.transform_result(records) != data_check.expected_result:
                raise ValueError(data_check.get_error_msg(records))
            logging.info(f"[result] Data check {i}: PASSED")
