import requests
import attr
from typing import List, Dict, Generator, Union
from lull_dagster_dbt.src.dbt_types import (
    DBTJob,
    DBTRunStatus,
    DBTRunStatusList,
    DBTRequestHeaders,
)
import time

from lull_dagster_dbt.src.dbt_exceptions import DBTNoJobIdException, DBTNoRunIdException

def is_none_or_empty(instance, attribute, value):
    if value is None or value == "":
        raise ValueError(f"{attribute.name} can't be None or Empty String")


@attr.s(auto_attribs=True)
class DBTApi:

    DBT_URL = "https://cloud.getdbt.com/api/v2/accounts"

    access_token: str = attr.ib(validator=is_none_or_empty)
    environment_id: int = attr.ib(validator=is_none_or_empty)
    account_id: int = attr.ib(validator=is_none_or_empty)
    project_id: int = attr.ib(validator=is_none_or_empty)

    base_url: str = attr.ib(
        default=attr.Factory(
            lambda self: f"{self.DBT_URL}/{self.account_id}",
            takes_self=True,
        )
    )
    headers: DBTRequestHeaders = attr.ib(
        default=attr.Factory(
            lambda self: {
                "Content-Type": "application/json",
                "Authorization": f"Token {self.access_token}",
            },
            takes_self=True,
        )
    )

    def request(
        self,
        method: str = "get",
        url: str = "/jobs",
        json: Union[Dict, None] = None,
        params: dict = None,
        return_type: Union[
            DBTRunStatus, DBTJob, DBTRunStatusList
        ] = DBTRunStatus,
    ):
        response = requests.request(
            method,
            self.base_url + url,
            headers=self.headers,
            json=json,
            params=params,
        )
        response.raise_for_status()
        return return_type.from_dict(response.json())

    def get_job(self, job_id: str) -> DBTJob:
        if job_id is None:
            raise DBTNoJobIdException("No Job ID provided")

        return self.request(url=f"/jobs/{job_id}", return_type=DBTJob)

    def get_job_runs(
        self, job_id: int = None, limit: int = 2
    ) -> DBTRunStatusList:
        if job_id is None:
            raise DBTNoJobIdException("No Job ID provided")

        return self.request(
            url="/runs",
            params={
                "include_related": ["job"],
                "job_definition_id": f"{job_id}",
                "order_by": "-id",
                "limit": limit,
            },
            return_type=DBTRunStatusList,
        )

    def create_run(
        self,
        job_id: str,
        cause: str = "Triggered by Dagster",
        steps_override: Union[List[str], None] = None,
    ) -> DBTRunStatus:
        if job_id is None:
            raise DBTNoJobIdException("No Job ID provided")

        data = {"cause": cause}

        if steps_override is not None and len(steps_override) > 0:
            data["steps_override"] = steps_override

        # trailing backslash required:
        # https://github.com/dbt-labs/dbt-cloud-openapi-spec/issues/7
        return self.request(
            method="post", url=f"/jobs/{job_id}/run/", json=data
        )

    def get_run(self, run_id: int = None) -> DBTRunStatus:
        if run_id is None:
            raise DBTNoRunIdException("Run ID Can't be None")

        return self.request(url=f"/runs/{run_id}")

    def cancel_run(self, run_id: int = None) -> DBTRunStatus:
        if run_id is None:
            raise DBTNoRunIdException("Run ID Can't be None")

        return self.request("post", f"/runs/{run_id}/cancel/")

    def trigger_and_wait(
        self,
        job_id: str,
        cause: str = "Triggered by Dagster",
        steps_override: list = None,
        time_limit_sec: int = 600,
        terminate_timed_out_run: bool = True,
        logger=None,
    ) -> Generator[DBTRunStatus, None, None]:
        """
        GENERATOR Method: Triggers a Job in DBT Cloud and waits for it to complete.
        This method yields a status and run_id with each request to DBT Cloud.
        """
        if job_id is None:
            raise DBTNoJobIdException("No Job ID provided")

        run_status = self.create_run(job_id, cause, steps_override)
        start = time.time()

        while run_status.is_running and (time.time() - start) < time_limit_sec:
            yield run_status
            time.sleep(30)

            if logger is not None:
                logger.info(f"Waiting on run: {run_status.run_id}, sleeping 30 seconds.")

            run_status = self.get_run(run_status.run_id)

        if run_status.is_running:
            if logger is not None:
                logger.warning(f"Run {run_status.run_id} did not finish.")
            run_status.timeout()

            if terminate_timed_out_run:
                if logger is not None:
                    logger.info(f"Cancelling run now.")

                self.cancel_run(run_status.run_id)

        yield run_status
