from airflow.plugins_manager import AirflowPlugin

import operators

# Defining the plugin class
class QualityPlugin(AirflowPlugin):
    name = "quality_plugin"
    operators = [
        operators.DataQualityOperator
    ]