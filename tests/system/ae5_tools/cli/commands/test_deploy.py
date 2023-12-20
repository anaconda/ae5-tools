import json

import pytest

from ae5_tools.api import AEUserSession
from tests.system.common import _get_account
from tests.utils import _cmd, _get_vars


@pytest.fixture(scope="session")
def user_session():
    hostname: str = _get_vars("AE5_HOSTNAME")
    local_account: dict = _get_account(id="1")
    username: str = local_account["username"]
    password: str = local_account["password"]
    s = AEUserSession(hostname, username, password)
    yield s
    s.disconnect()


@pytest.fixture(scope="module")
def project_list(user_session):
    return _cmd("project", "list", "--collaborators")


@pytest.fixture(scope="module")
def cli_project(project_list):
    return next(rec for rec in project_list if rec["name"] == "testproj3")


###############################################################################
# <owner>/<name>:<revision> tests
###############################################################################


def test_deploy_by_owner_and_name_project_latest_implicit(cli_project):
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
        "--wait",
    )
    _cmd("deployment", "stop", drec["id"])

    revision: str = drec["revision"]
    assert revision == "latest"


def test_deploy_by_owner_and_name_project_latest_explicit(cli_project):
    prec = cli_project
    dname = "testdeploy"
    ename = "testendpoint"
    drec = _cmd(
        "project",
        "deploy",
        f'{prec["owner"]}/{prec["name"]}:latest',
        "--name",
        dname,
        "--endpoint",
        ename,
        "--command",
        "default",
        "--private",
        "--wait",
    )
    _cmd("deployment", "stop", drec["id"])

    revision: str = drec["revision"]
    assert revision == "latest"


def test_deploy_by_owner_and_name_project_first_explicit(cli_project):
    prec = cli_project
    dname = "testdeploy"
    ename = "testendpoint"
    drec = _cmd(
        "project",
        "deploy",
        f'{prec["owner"]}/{prec["name"]}:0.1.0',
        "--name",
        dname,
        "--endpoint",
        ename,
        "--command",
        "default",
        "--private",
        "--wait",
    )
    _cmd("deployment", "stop", drec["id"])

    revision: str = drec["revision"]
    assert revision == "0.1.0"


###############################################################################
# <id>:<revision> tests
###############################################################################


def test_deploy_by_id_and_revision_project_latest_implicit(cli_project):
    prec = cli_project
    dname = "testdeploy"
    ename = "testendpoint"
    drec = _cmd(
        "project",
        "deploy",
        f'{prec["id"]}',
        "--name",
        dname,
        "--endpoint",
        ename,
        "--command",
        "default",
        "--private",
        "--wait",
    )
    _cmd("deployment", "stop", drec["id"])

    revision: str = drec["revision"]
    assert revision == "latest"


def test_deploy_by_id_and_revision_project_latest_explicit(cli_project):
    prec = cli_project
    dname = "testdeploy"
    ename = "testendpoint"
    drec = _cmd(
        "project",
        "deploy",
        f'{prec["id"]}:latest',
        "--name",
        dname,
        "--endpoint",
        ename,
        "--command",
        "default",
        "--private",
        "--wait",
    )
    _cmd("deployment", "stop", drec["id"])

    revision: str = drec["revision"]
    assert revision == "latest"


def test_deploy_by_id_and_revision_project_first_explicit(cli_project):
    prec = cli_project
    dname = "testdeploy"
    ename = "testendpoint"
    drec = _cmd(
        "project",
        "deploy",
        f'{prec["id"]}:0.1.0',
        "--name",
        dname,
        "--endpoint",
        ename,
        "--command",
        "default",
        "--private",
        "--wait",
    )
    _cmd("deployment", "stop", drec["id"])

    revision: str = drec["revision"]
    assert revision == "0.1.0"
