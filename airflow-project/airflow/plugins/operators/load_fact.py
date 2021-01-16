import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from psycopg2 import sql


class LoadFactOperator(BaseOperator):
    """Operator to data into a fact table

    Attributes:
        columns: Table columns to populate (must match select query order)
        redshift_conn_id: Name of Airflow connection for Redshift
        select_query: Query to select staging data into fact table
        target_table: Fact table to insert data
    """
    ui_color = "#F98866"

    insert_sql_template = "INSERT INTO {target_table} ({columns}) ({select_sql});"

    @apply_defaults
    def __init__(self,
                 columns=[],
                 redshift_conn_id="",
                 select_sql="",
                 target_table="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.target_table = target_table

    def execute(self, context):
        logging.info(f"Loading data into {self.target_table} fact table.")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        insert_query = sql.SQL(self.insert_sql_template).format(
            columns=sql.SQL(",").join([sql.Identifier(col) for col in self.columns]),
            select_sql=sql.SQL(self.select_sql),
            target_table=sql.Identifier(self.target_table),
        )
        redshift.run([insert_query])
