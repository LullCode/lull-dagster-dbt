import pytest, pdb
from unittest.mock import patch, Mock
from dbt.src.dbt_api import DBTApi
from dbt.src.dbt_exceptions import DBTNoJobIdException, DBTNoRunIdException
from dbt.src.dbt_types import DBTJob, DBTRunStatus
from tests.fixtures.dbt_fixtures import get_run_success, get_run_running


@pytest.fixture
def dbt_obj():
    return DBTApi("test")


class TestDBTApi:
    def test___init___set(self, dbt_obj):
        dbt = DBTApi("test")
        assert dbt.headers["Authorization"] == "Token test"

    def test___init___exception(self):
        with pytest.raises(ValueError) as exec_info:
            DBTApi(access_token=None)

        assert (
            str(exec_info.value)
            == "access_token can't be None or Empty String"
        )

    @patch("dbt.src.dbt_api.requests.request")
    def test_request(self, dbt_req_get, dbt_obj):
        response_mock = Mock()
        json_mock = Mock()
        raise_for_status_mock = Mock()
        response_mock.json = json_mock
        response_mock.raise_for_status = raise_for_status_mock
        dbt_req_get.return_value = response_mock
        from_dict_mock = Mock()
        return_type_mock = Mock(from_dict=from_dict_mock)

        dbt_obj.request(return_type=return_type_mock)

        dbt_req_get.assert_called_once_with(
            "get",
            "https://cloud.getdbt.com/api/v2/accounts/1128/jobs",
            headers=dbt_obj.headers,
            json=None,
            params=None,
        )
        raise_for_status_mock.assert_called_once()
        json_mock.assert_called_once()
        from_dict_mock.assert_called_once()

    @patch("dbt.src.dbt_api.DBTApi.request")
    def test_get_job_success(self, req_func, dbt_obj):
        dbt_obj.get_job("1234")
        req_func.assert_called_once_with(url="/jobs/1234", return_type=DBTJob)

    def test_get_job_exception(self, dbt_obj):
        with pytest.raises(DBTNoJobIdException) as exec_info:
            dbt_obj.get_job(None)

        assert str(exec_info.value) == "No Job ID provided"

    @pytest.mark.parametrize(
        "job_id,keyargs,jsonfield",
        [
            (
                "test",
                {"cause": "test", "steps_override": ["testing"]},
                {
                    "url": "/jobs/test/run/",
                    "method": "post",
                    "json": {"cause": "test", "steps_override": ["testing"]},
                },
            ),
            (
                "test",
                {"cause": "test"},
                {
                    "url": "/jobs/test/run/",
                    "method": "post",
                    "json": {"cause": "test"},
                },
            ),
            (
                "test",
                {},
                {
                    "url": "/jobs/test/run/",
                    "method": "post",
                    "json": {"cause": "Triggered by Dagster"},
                },
            ),
        ],
    )
    @patch("dbt.src.dbt_api.DBTApi.request")
    def test_create_run_success(
        self, req_func, dbt_obj, job_id, keyargs, jsonfield
    ):
        dbt_obj.create_run(job_id, **keyargs)
        req_func.assert_called_once_with(**jsonfield)

    def test_create_run_exception(self, dbt_obj):
        with pytest.raises(DBTNoJobIdException) as exec_info:
            dbt_obj.create_run(None)

        assert str(exec_info.value) == "No Job ID provided"

    @patch("dbt.src.dbt_api.DBTApi.request")
    def test_get_run_success(self, req_func, dbt_obj):
        dbt_obj.get_run(1234)
        req_func.assert_called_once_with(url="/runs/1234")

    def test_get_run_exception(self, dbt_obj):
        with pytest.raises(DBTNoRunIdException) as exec_info:
            dbt_obj.get_run(None)

        assert str(exec_info.value) == "Run ID Can't be None"

    @patch("dbt.src.dbt_api.time.sleep", return_value=None)
    @patch("dbt.src.dbt_api.time.time")
    @patch("dbt.src.dbt_api.DBTApi.get_run")
    @patch("dbt.src.dbt_api.DBTApi.create_run")
    @patch("dbt.src.dbt_api.DBTApi.cancel_run")
    def test_trigger_and_wait(
        self,
        cancel_run_mock,
        create_run_mock,
        get_run_mock,
        time_mock,
        sleep_mock,
        dbt_obj,
        get_run_success,
        get_run_running,
    ):
        time_mock.return_value = 0

        run_id = get_run_running["data"]["id"]
        job_id = get_run_running["data"]["job"]["id"]
        create_run_mock.return_value = DBTRunStatus.from_dict(get_run_running)

        get_run_returns = [
            DBTRunStatus.from_dict(get_run_running),
            DBTRunStatus.from_dict(get_run_success),
        ]
        get_run_mock.side_effect = get_run_returns

        for res in dbt_obj.trigger_and_wait(job_id, "test", []):
            pass

        create_run_mock.assert_called_once_with(job_id, "test", [])
        assert get_run_mock.call_count == len(get_run_returns)

    @patch("dbt.src.dbt_api.time.sleep", return_value=None)
    @patch("dbt.src.dbt_api.time.time")
    @patch("dbt.src.dbt_api.DBTApi.get_run")
    @patch("dbt.src.dbt_api.DBTApi.create_run")
    @patch("dbt.src.dbt_api.DBTApi.cancel_run")
    @pytest.mark.parametrize("to_cancel_run", [True, False])
    def test_trigger_and_wait_cancel_run(
        self,
        cancel_run_mock,
        create_run_mock,
        get_run_mock,
        time_mock,
        sleep_mock,
        dbt_obj,
        get_run_running,
        to_cancel_run
    ):
        # two more than the get_run_returns list,
        # last value exits while loop:
        time_mock.side_effect = [0, 0, 0, 2]

        run_id = get_run_running["data"]["id"]
        job_id = get_run_running["data"]["job"]["id"]
        create_run_mock.return_value = DBTRunStatus.from_dict(get_run_running)

        get_run_returns = [
            DBTRunStatus.from_dict(get_run_running),
            DBTRunStatus.from_dict(get_run_running), # job did not finish
        ]
        get_run_mock.side_effect = get_run_returns

        for res in dbt_obj.trigger_and_wait(job_id, "test", [], terminate_timed_out_run=to_cancel_run, time_limit_sec=1):
            pass

        create_run_mock.assert_called_once_with(job_id, "test", [])
        assert get_run_mock.call_count == len(get_run_returns)

        if to_cancel_run:
            cancel_run_mock.assert_called_once_with(run_id)

    @patch("dbt.src.dbt_api.DBTApi.request")
    def test_cancel_run_success(self, req_func, dbt_obj):
        dbt_obj.cancel_run(1234)
        req_func.assert_called_once_with("post", "/runs/1234/cancel/")

    def test_cancel_run_exception(self, dbt_obj):
        with pytest.raises(DBTNoRunIdException) as exec_info:
            dbt_obj.cancel_run(None)

        assert str(exec_info.value) == "Run ID Can't be None"