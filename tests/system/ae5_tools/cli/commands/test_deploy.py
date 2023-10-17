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


###############################################################################
# <owner>/<name>:<revision> tests
###############################################################################


@pytest.mark.skip(reason="failing against ci")
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


@pytest.mark.skip(reason="failing against ci")
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


@pytest.mark.skip(reason="failing against ci")
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


@pytest.mark.skip(reason="failing against ci")
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


@pytest.mark.skip(reason="failing against ci")
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


@pytest.mark.skip(reason="failing against ci")
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
