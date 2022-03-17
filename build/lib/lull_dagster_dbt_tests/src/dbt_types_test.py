import pytest

from dbt.src.dbt_types import (
    DBTRequestStatus,
    DBTJob,
    DBTRunStatus,
    DBTRunStatusList,
)

from dbt.src.dbt_exceptions import DBTRunTimeoutException

from tests.fixtures.dbt_fixtures import (
    get_job,
    get_run_running,
    get_run_success,
    get_run_status_list,
)


class TestDBTTypes:
    def test_job(self, get_job):
        # This checks that an error isn't thrown and can accept response
        DBTJob.from_dict(get_job)

    def test_job_ignore_default(self, get_run_success):
        # How the run status stores the job:
        DBTJob.from_dict(get_run_success["data"]["job"], ignore_default=True)

    def test_run_status(self, get_run_success):
        run_status = DBTRunStatus.from_dict(get_run_success)

        assert run_status.status_map == "Success"
        assert not run_status.run_failed
        assert run_status.run_succeeded
        assert not run_status.is_running

    def test_run_status_timeout(self, get_run_success):
        run_status = DBTRunStatus.from_dict(get_run_success)
        run_status.timeout()

        assert not run_status.is_running
        assert run_status.run_timed_out
        assert run_status.status == -1
        assert run_status.status_humanized == "Timeout"


    def test_run_status_check_run_progress_timed_out(self, get_run_success):
        run_status = DBTRunStatus.from_dict(get_run_success)
        run_status.timeout()

        with pytest.raises(DBTRunTimeoutException) as exec_info:
            run_status.check_run_progress()

        assert str(exec_info.value) == f"Run {run_status.run_id} failed to complete, status: {run_status.status_humanized}"

    def test_run_status_check_run_progress_succeeded(self, get_run_success):
        run_status = DBTRunStatus.from_dict(get_run_success)
        # no exception

    def test_run_status_check_run_progress_not_running(self, get_run_running):
        run_status = DBTRunStatus.from_dict(get_run_running)
        run_status.is_running = False

        with pytest.raises(Exception) as exec_info:
            run_status.check_run_progress()

        assert str(exec_info.value) == f"Job {run_status.run_id} failed with status: {run_status.status_humanized}"


    def test_run_status_list(self, get_run_status_list):
        run_status_list = DBTRunStatusList.from_dict(get_run_status_list)

        assert len(run_status_list.run_list) == len(
            get_run_status_list["data"]
        )

        assert run_status_list.get_last_completed_status() == "Success"

    def test_run_status_list_running(self, get_run_status_list):
        # current run is still running:
        get_run_status_list["data"][0]["status"] = 1

        run_status_list = DBTRunStatusList.from_dict(get_run_status_list)

        # Checks previous run:
        assert run_status_list.get_last_completed_status() == "Success"
