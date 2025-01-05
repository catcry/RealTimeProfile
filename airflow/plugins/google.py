from airflow.plugins_manager import AirflowPlugin
from airflow.models.baseoperator import BaseOperator

class GoogleLink(BaseOperator):
    name = "google"

    def get_link(self, operator, dttm):
        return "https://www.google.com"
    

class AirflowTestPlugin(AirflowPlugin):
    name = "google"
    global_operator_extra_links = [GoogleLink(), ]