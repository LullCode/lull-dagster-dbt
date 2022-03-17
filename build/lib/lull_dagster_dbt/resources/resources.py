from dagster import resource
import os
from lull_dagster_dbt.src.dbt_api import DBTApi


@resource
def dbt_interface(init_context):
    return DBTApi(os.environ.get("DBT_ACCESS_TOKEN"))
