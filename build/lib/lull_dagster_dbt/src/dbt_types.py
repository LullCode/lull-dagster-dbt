from __future__ import annotations
from typing import List, Dict, Union
from typing_extensions import TypedDict
import attr
from helpers.attr_serialization import attr_serialization
from lull_dagster_dbt.src.dbt_exceptions import DBTRunTimeoutException

class DBTApiResponseDict(TypedDict):
    data: Union[List, Dict]
    status: Dict
    extra: Union[Dict, None]


DBTRequestHeaders = TypedDict(
    "RequestHeadersType", {"Content-Type": str, "Authorization": str}
)


# pylint: disable=too-few-public-methods
@attr_serialization
@attr.s(kw_only=True)
class DBTRequestStatus:
    """
    This is typically set an included in the result of one of the other classes.
    """

    code: int
    is_success: bool
    user_message: str
    developer_message: str


# pylint: disable=too-many-instance-attributes
@attr_serialization(from_default=["data"])
@attr.s
class DBTJob:
    """
    The DBTJob class is typically included as a part of the run information
    and is usually housed as an attribute on the DBTRunStatus class. It
    contains information about the state of the project and the job itself.
    It also includes it's own reference to the status of the API request
    """

    account_id: int = attr.ib(kw_only=True)
    project_id: int = attr.ib(kw_only=True)
    environment_id: int = attr.ib(kw_only=True)
    name: str = attr.ib(kw_only=True)
    dbt_version: str = attr.ib(kw_only=True)
    execute_steps: List[str] = attr.ib(kw_only=True)
    threads: int = attr.ib(metadata={"from": ["settings", "threads"]})
    target_name: str = attr.ib(metadata={"from": ["settings", "target_name"]})
    state: int = attr.ib(kw_only=True)
    generate_docs: bool = attr.ib(kw_only=True)
    cron: str = attr.ib(metadata={"from": ["schedule", "cron"]})
    schedule_date: str = attr.ib(metadata={"from": ["schedule", "date"]})
    schedule_time: str = attr.ib(metadata={"from": ["schedule", "time"]})
    request_status: DBTRequestStatus = attr.ib(
        kw_only=True,
        metadata={"from": ["status"], "ignore_default": True},
        converter=lambda x: DBTRequestStatus.from_dict(x)
        if x is not None
        else None,
        default=None,
    )


# pylint: disable=too-many-instance-attributes
# pylint: disable=too-few-public-methods
@attr_serialization(from_default=["data"])
@attr.s()
class DBTRunStatus:
    """
    This class holds the results of an API call that returns
    DBT Run information. All the fields returned in the API
    are mapped here. Either in subclasses as attributes or in
    other attributes.
    """

    # official mapping from API Docs
    # 1: "Queued",
    # 2: "Starting",
    # 3: "Running",
    # 10: "Success",
    # 20: "Error",
    # 30: "Cancelled"
    # -1: "Manual Timeout" is a custom status for the trigger_and_wait method
    RUN_STATUS_REMAPPED: Dict[int, str] = {
        -1: "Manual Timeout",
        1: "Running",
        2: "Running",
        3: "Running",
        10: "Success",
        20: "Error",
        30: "Error",
    }

    # This is the name as it's returned from the DBT API
    # pylint: disable=invalid-name
    id: int = attr.ib(kw_only=True)

    run_id: int = attr.ib(
        kw_only=True,
        default=attr.Factory(lambda self: self.id, takes_self=True),
    )

    trigger_id: int = attr.ib(kw_only=True)
    account_id: int = attr.ib(kw_only=True)
    project_id: int = attr.ib(kw_only=True)
    job_definition_id: int = attr.ib(kw_only=True)
    status: int = attr.ib(kw_only=True)
    git_branch: str = attr.ib(kw_only=True)
    git_sha: str = attr.ib(kw_only=True)
    status_message: str = attr.ib(kw_only=True)
    dbt_version: str = attr.ib(kw_only=True)
    # datetimes convert?
    created_at: str = attr.ib(kw_only=True)
    updated_at: str = attr.ib(kw_only=True)
    dequeued_at: str = attr.ib(kw_only=True)
    started_at: str = attr.ib(kw_only=True)
    finished_at: str = attr.ib(kw_only=True)
    last_checked_at: str = attr.ib(kw_only=True)
    last_heartbeat_at: str = attr.ib(kw_only=True)
    owner_thread_id: str = attr.ib(kw_only=True)
    executed_by_thread_id: str = attr.ib(kw_only=True)
    artifacts_saved: bool = attr.ib(kw_only=True)
    artifact_s3_path: str = attr.ib(kw_only=True)
    has_docs_generated: bool = attr.ib(kw_only=True)
    # the job dict
    job: Union[DBTJob, None] = attr.ib(
        kw_only=True,
        converter=lambda x: DBTJob.from_dict(x, ignore_default=True)
        if x is not None
        else None,
        default=None,
        metadata={"from": ["job"]},
    )
    duration: str = attr.ib(kw_only=True)
    queued_duration: str = attr.ib(kw_only=True)
    run_duration: str = attr.ib(kw_only=True)
    duration_humanized: str = attr.ib(kw_only=True)
    queued_duration_humanized: str = attr.ib(kw_only=True)
    run_duration_humanized: str = attr.ib(kw_only=True)
    status_humanized: str = attr.ib(kw_only=True)
    created_at_humanized: str = attr.ib(kw_only=True)
    # the status dict
    request_status: DBTRequestStatus = attr.ib(
        kw_only=True,
        metadata={"from": ["status"], "ignore_default": True},
        converter=DBTRequestStatus.from_dict,
    )
    status_map: int = attr.ib(init=False)
    run_failed: bool = attr.ib(init=False)
    run_succeeded: bool = attr.ib(init=False)
    is_running: bool = attr.ib(init=False)
    run_timed_out: bool = attr.ib(default=False)

    def __attrs_post_init__(self):
        self.status_map = self.RUN_STATUS_REMAPPED[self.status]

        self.run_failed = self.status_map == "Error"
        self.run_succeeded = self.status_map == "Success"
        self.is_running = self.status_map == "Running"

    def timeout(self):
        self.is_running = False
        self.run_timed_out = True
        self.status = -1
        self.status_humanized = "Timeout"

    def check_run_progress(self, logger=None):
        if self.run_timed_out:
            raise DBTRunTimeoutException(
                f"Run {self.run_id} failed to complete, status: {self.status_humanized}"
            )
        elif self.run_succeeded:
            if logger is not None:
                logger.info(f"Run succeeded: {self.run_id}")
        elif not self.is_running:
            raise Exception(
                f"Job {self.run_id} failed with status: {self.status_humanized}"
            )



@attr.s
class DBTRunStatusList:
    """
    This class is designed to hold a list of DBTRunStatuses.
    This gets set and instantiated from the `get_job_runs` method in the
    api. In practice, this is used to find the last completed job run.
    See: `get_last_completed_status`
    """

    run_list: List[DBTRunStatus] = attr.ib(kw_only=True)

    def get_last_completed_status(self) -> str:
        """
        This only looks at the last two runs listed here to get their status
        for reporting reasons.
        """
        if len(self.run_list) > 1 and self.run_list[0].is_running:
            return self.run_list[1].status_humanized

        return self.run_list[0].status_humanized

    @classmethod
    def from_dict(cls, dbt_response: DBTApiResponseDict) -> DBTRunStatusList:
        """
        Creates a List of DBTRunStatuses. This is used to examine previous
        job executions and is used by Dagster to examine and report on the
        last completed job run.
        """
        return cls(
            run_list=[
                DBTRunStatus.from_dict(
                    {"data": item, "status": dbt_response["status"]}
                )
                for item in dbt_response["data"]
            ]
        )
