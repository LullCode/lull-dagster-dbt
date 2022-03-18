from dagster import resource
import os
from lull_dagster_dbt.src.dbt_api import DBTApi


@resource
def dbt_interface(init_context):
    return DBTApi(
        access_token=os.environ.get("DBT_ACCESS_TOKEN"),
        environment_id=os.environ.get("DBT_ENVIRONMENT_ID"),
        account_id=os.environ.get("DBT_ACCOUNT_ID"),
        project_id=os.environ.get("DBT_PROJECT_ID"),
    )
