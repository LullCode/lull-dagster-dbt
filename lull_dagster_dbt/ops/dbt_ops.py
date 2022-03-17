from typing import List
from lull_dagster_dbt.src import DBTApi
import requests

from dagster import (
    DynamicOut,
    Any,
    op,
    In,
    Out,
    RetryPolicy,
    Backoff,
    DynamicOutput,
)

from datetime import datetime, timezone


@op(
    ins={
        "job_id": In(
            description="The DBT Cloud Job Id to trigger. This can be found "
            "by clicking on the job in DBT Cloud and retrieving from "
            "the URL or description"
        ),
        "cause": In(
            description="The cause for invoking this job, gets defaulted "
            'to "Triggered by Dagster". Set in case more info is needed.'
        ),
        "steps_override": In(
            description=(
                "This is an optional list of strings that designates what "
                "dbt steps to run. It defaults to what the job was set with."
                """
                E.g.
                [ 'dbt run --models "+marketing_gross_profit"',
                'dbt docs generate' ]
                """
            )
        ),
        "time_limit_sec": In(
            description="Time limit in seconds to wait for dbt job, "
            "default is 900s (15 mins)"
        ),
        "run_after": In(
            description="Trigger to have this run after another job"
        ),
        "run_after_list": In(),
        "terminate_timed_out_run": In(
            description="Whether to cancel the DBT if it does not complete within the time_limit_sec."
        )
    },
    retry_policy=RetryPolicy(
        max_retries=3, delay=5, backoff=Backoff("EXPONENTIAL")
    ),
    required_resource_keys={"dbt_interface"},
)
def dbt_trigger_and_wait(
    context,
    job_id: int,
    cause: str,
    steps_override: list = [],
    time_limit_sec: int = 900,
    run_after: Any = None,
    run_after_list: list = [],
    terminate_timed_out_run: bool = True
):
    dbt: DBTApi = context.resources.dbt_interface

    for run_status in dbt.trigger_and_wait(
        job_id, 
        cause, 
        steps_override, 
        time_limit_sec, 
        logger=context.log, 
        terminate_timed_out_run=terminate_timed_out_run
    ):  
        run_status.check_run_progress(context.log)

@op(
    ins={
        "job_id": In(
            description="The DBT Cloud Job Id. This can be found by "
            "clicking on the job in DBT Cloud and retrieving from "
            "the URL or description",
        ),
    },
    out={
        "job_status": Out(
            description=(
                "Return from the DBT Cloud API detailing information "
                "about the run id"
            )
        )
    },
    retry_policy=RetryPolicy(
        max_retries=3, delay=5, backoff=Backoff("EXPONENTIAL")
    ),
    required_resource_keys={"dbt_interface"},
)
def dbt_validate(context, job_id: int) -> str:
    dagster_dbt: DBTApi = context.resources.dbt_interface
    run = dagster_dbt.get_job_runs(job_id).run_list[0]

    if run.request_status.code == requests.codes.ok:
        finished_at = datetime.strptime(
            run.finished_at,
            "%Y-%m-%d %H:%M:%S.%f%z",
        )
        finished_at = finished_at.replace(tzinfo=timezone.utc).astimezone(
            tz=None
        )
        finished_at = finished_at.strftime("%Y-%m-%d %I:%M:%S %p")

        mark = ":x:" if run.run_failed else ":white_check_mark:"
        return f"{run.job.name} finished at {finished_at} {mark}:"

    return (
        f"Failed to validate job: {run.job.name}, "
        f"response code: {run.request_status.code}"
    )


@op(
    ins={
        "dbt_jobs_ids": In(
            description="A list of DBT Cloud Job Ids. These can be found "
            "by clicking on the jobs in DBT Cloud and retrieving from "
            "the URL or description",
        )
    },
    out={"job_id": DynamicOut(description="An individual DBT Cloud Job Id.")},
)
def dbt_uptime_get_yaml_config(dbt_jobs_ids: List[int]) -> int:
    for job_id in dbt_jobs_ids:
        yield DynamicOutput(
            output_name="job_id", value=job_id, mapping_key=str(job_id)
        )

