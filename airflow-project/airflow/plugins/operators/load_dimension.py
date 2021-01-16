import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from psycopg2 import sql


class LoadDimensionOperator(BaseOperator):
    """Operator to load data into a dimension table

    Attributes:
        append_only: Append new data to table or truncate before inserting new data
        columns: Table columns to populate (must match select query order)
        redshift_conn_id: Name of Airflow connection for Redshift
        select_sql: Query to select staging data into dimension table
        target_table: Dimension table to insert data
    """
    ui_color = "#80BD9E"

    truncate_sql_template = "TRUNCATE TABLE {target_table};"
    insert_sql_template = "INSERT INTO {target_table} ({columns}) ({select_sql})"

    @apply_defaults
    def __init__(self,
                 append_only=False,
                 columns=[],
                 redshift_conn_id="",
                 select_sql="",
                 target_table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.append_only = append_only
        self.columns = columns
        self.redshift_conn_id = redshift_conn_id
        self.select_sql = select_sql
        self.target_table = target_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # Query for insert data into dimension table
        insert_query = sql.SQL(self.insert_sql_template).format(
            columns=sql.SQL(",").join([sql.Identifier(col) for col in self.columns]),
            select_sql=sql.SQL(self.select_sql),
            target_table=sql.Identifier(self.target_table),
        )

        if self.append_only:
            logging.info(f"Inserting data into {self.target_table} dimension table in append mode.")
            redshift.run([insert_query])
            return

        # If append only is false, the table will be truncated before insertion
        logging.info(f"Inserting data into {self.target_table} dimension table in delete/insert mode.")
        redshift.run([
            sql.SQL(self.truncate_sql_template).format(
                target_table=sql.Identifier(self.target_table),
            ),
            insert_query,
        ])
