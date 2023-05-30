# import time
# from typing import Dict, List
#
# import pytest
#
# from ae5_tools.api import AEUserSession
# from tests.utils import _cmd, _get_vars
#
#
# @pytest.fixture(scope="session")
# def user_session():
#     hostname, username, password = _get_vars("AE5_HOSTNAME", "AE5_USERNAME", "AE5_PASSWORD")
#     s = AEUserSession(hostname, username, password)
#     yield s
#     s.disconnect()
#
#
# @pytest.fixture(scope="module")
# def project_list(user_session):
#     return _cmd("project", "list", "--collaborators")
#
#
# @pytest.fixture(scope="module")
# def cli_project(project_list):
#     return next(rec for rec in project_list if rec["name"] == "testproj3")
#
#
# def test_job_run(cli_project):
#     # Set up the test
#
#     # Create a pre-existing job, (run it and wait for completion)
#     prec = cli_project
#     create_job_result: Dict = _cmd(
#         "job", "create", prec["id"], "--name", "testjob1", "--command", "run", "--run", "--wait"
#     )
#
#     # Execute the test (Run a previously created job)
#     run_job_result: Dict = _cmd("job", "run", "testjob1")
#
#     # Review Test Results
#     assert run_job_result["name"] == "testjob1"
#     assert run_job_result["project_name"] == "testproj3"
#
#     # Ensure the new triggered run completes.
#     wait_time: int = 5
#     counter: int = 0
#     max_loop: int = 100
#     wait: bool = True
#     while wait:
#         run_once_status: Dict = _cmd("run", "info", run_job_result["id"])
#         if run_once_status["state"] == "completed":
#             wait = False
#         else:
#             counter += 1
#             time.sleep(wait_time)
#             if counter > max_loop:
#                 wait = False
#     assert counter < max_loop
#
#     # Cleanup after the test
#
#     # Remove runs
#     job_runs: List[Dict] = _cmd("job", "runs", create_job_result["id"])
#     for run in job_runs:
#         _cmd("run", "delete", run["id"])
#
#     # Remove job
#     _cmd("job", "delete", create_job_result["id"])
