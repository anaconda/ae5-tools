import time
from typing import Dict, List

import pytest

from ae5_tools.api import AEUserSession
from tests.utils import _cmd, _get_vars


@pytest.fixture(scope="session")
def user_session():
    hostname, username, password = _get_vars("AE5_HOSTNAME", "AE5_USERNAME", "AE5_PASSWORD")
    s = AEUserSession(hostname, username, password)
    yield s
    s.disconnect()

@pytest.fixture(scope="module")
def project_list(user_session):
    return _cmd("project", "list", "--collaborators")


@pytest.fixture(scope="module")
def cli_project(project_list):
    return next(rec for rec in project_list if rec["name"] == "testproj3")


@pytest.fixture(scope="module")
def test_deploy_project(cli_project):
    prec = cli_project
    dname = "testdeploy"
    ename = "testendpoint"
    drec = _cmd(
        "project",
        "deploy",
        f'{prec["owner"]}/{prec["name"]}',
        "--name",
        dname,
        "--endpoint",
        ename,
        "--command",
        "default",
        "--private",
    )
    print(drec)
    _cmd("deployment", "stop", drec["id"])


