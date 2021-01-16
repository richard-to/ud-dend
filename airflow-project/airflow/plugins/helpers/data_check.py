from psycopg2 import sql


def noop(results):
    """Noop transform

    Args:
        results: Results from Redshift query

    Returns:
        The same results unchanged
    """
    return results


class DataCheck:
    """Check that data is valid given a SQL query

    Attributes:
        name: Description of the data quality check
        error_msg: Msg to display when a check fails
        expected_result: Expected result after transformation
        sql_query: Query to run for checking data
        transform_result: Transforms the results from the Redshift query
    """
    def __init__(
        self,
        name="",
        error_msg="Data check failed! Expected {transformed_result} to equal {expected_result}!",
        expected_result="",
        sql_query="",
        transform_result=noop,
    ):
        self.name = name
        self.error_msg = error_msg
        self.expected_result = expected_result
        self.sql_query = sql_query
        self.transform_result = transform_result

    def get_error_msg(self, result):
        """Generates an error message

        Args:
            results: Results from Redshift query

        Returns:
            A formatted message describing the data check error
        """
        return self.error_msg.format(
            transformed_result=self.transform_result(result),
            expected_result=self.expected_result,
        )


class DataCheckFactory:
    """Helper class that creates common data checks"""

    @staticmethod
    def create_no_data_check(table, col):
        """Creates a check that tests that a table has at least 1 row

        Args:
            table: Name of table to check
            col: Name of column to count (Redshift doesn't allow '*')

        Return:
            DataCheck
        """
        return DataCheck(
            name=f"Check {table} table has data",
            sql_query=sql.SQL("SELECT COUNT({col}) FROM {table}").format(
                table=sql.Identifier(table),
                col=sql.Identifier(col),
            ),
            transform_result=lambda rows: rows[0][0] > 0,
            expected_result=True,
        )

    @staticmethod
    def create_duplicates_check(table, id_col):
        """Creates a check that tests for duplicate rows in a table

        Since Redshift does not enforce primary key constraints, we'll need to
        check that the primary key for a table does not appear multiple times.

        Args:
            table: Name of table to check
            id_col: Primary key column that should be unique

        Return:
            DataCheck
        """
        return DataCheck(
            name=f"Check duplicate on {table} table",
            sql_query=sql.SQL(
                "SELECT COUNT({col}) FROM {table} GROUP BY {col} HAVING COUNT({col}) > 1"
            ).format(table=sql.Identifier(table), col=sql.Identifier(id_col)),
            transform_result=len,
            expected_result=0,
        )

    @staticmethod
    def create_valid_values_check(table, col, expected_values):
        """Creates a check that tests that column only contains certain values

        Since we can't enforce check constraints in Redshift, we'll need to
        verify that certain columns only contain valid values.

        An example is the users table which has a level column that should
        only contain the values `free` or `paid`.

        Args:
            table: Name of table to check
            col: Column to check values
            expected_values: List of allowed values

        Return:
            DataCheck
        """
        return DataCheck(
            name=f"Check valid values for {table}.{col} are {expected_values}",
            sql_query=sql.SQL("SELECT DISTINCT {col} FROM {table}").format(
                table=sql.Identifier(table),
                col=sql.Identifier(col),
            ),
            transform_result=lambda rows: set([r[0] for r in rows]),
            expected_result=set(expected_values),
        )
