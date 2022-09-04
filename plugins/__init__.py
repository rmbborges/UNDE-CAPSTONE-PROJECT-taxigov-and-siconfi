from __future__ import division, absolute_import, print_function

from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class DataQualityPlugin(AirflowPlugin):
    name = "data_quality_plugin"
    operators = [
        operators.UpstreamDependencyCheckOperator,
        operators.UniqueKeyCheckOperator
    ]