from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import helpers
import operators


# Defining the plugin class
class UdacityPlugin(AirflowPlugin):
    name = "udacity_plugin"
    operators = [
        operators.DataQualityOperator,
        operators.LoadDimensionOperator,
        operators.LoadFactOperator,
        operators.StageToRedshiftOperator,
    ]
    helpers = [
        helpers.DataCheck,
        helpers.DataCheckFactory,
        helpers.SqlQueries,
    ]
