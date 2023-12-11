import glob
import os
import pprint
import tarfile
import tempfile
import time
from datetime import datetime

import pytest
import requests

from ae5_tools.api import AEUnexpectedResponseError
from tests.utils import CMDException, _cmd, _compare_tarfiles


@pytest.fixture(scope="module")
def project_list(user_session):
    return _cmd("project", "list", "--collaborators")


def test_project_info(project_list):
    for rec0 in project_list:
        id = rec0["id"]
        pair = "{}/{}".format(rec0["owner"], rec0["name"])
        rec1 = _cmd("project", "info", id)
        rec2 = _cmd("project", "info", pair)
        rec3 = _cmd("project", "info", f"{pair}/{id}")
        assert all(rec0[k] == v for k, v in rec2.items()), pprint.pformat((rec0, rec2))
        assert all(rec1[k] == v for k, v in rec2.items()), pprint.pformat((rec1, rec2))
        assert rec2 == rec3


def test_project_info_errors(project_list):
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "info", "testproj1")
    assert "Multiple projects" in str(excinfo.value)
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "info", "testproj4")
    assert "No projects" in str(excinfo.value)


@pytest.fixture(scope="module")
def resource_profiles(user_session):
    return _cmd("resource-profile", "list")


def test_resource_profiles(resource_profiles):
    for rec in resource_profiles:
        rec2 = _cmd("resource-profile", "info", rec["name"])
        assert rec == rec2
    # Dropping because the * is getting expanded for some reason in the tests
    # with pytest.raises(CMDException) as excinfo:
    #     _cmd('resource-profile', 'info', '*')
    # assert 'Multiple resource profiles found' in str(excinfo.value)
    with pytest.raises(CMDException) as excinfo:
        _cmd("resource-profile", "info", "abcdefg")
    assert "No resource profiles found" in str(excinfo.value)


@pytest.fixture(scope="module")
def editors(user_session):
    return _cmd("editor", "list")


def test_editors(editors):
    for rec in editors:
        assert rec == _cmd("editor", "info", rec["id"])
    assert sum(rec["is_default"].lower() == "true" for rec in editors) == 1
    assert set(rec["id"] for rec in editors).issuperset({"jupyterlab", "notebook"})


@pytest.mark.xfail
def test_endpoints():
    slist = _cmd("endpoint", "list")
    for rec in slist:
        rec2 = _cmd("endpoint", "info", rec["id"])
        assert rec == rec2


def test_samples():
    slist = _cmd("sample", "list")
    assert sum(rec["is_default"].lower() == "true" for rec in slist) == 1
    assert sum(rec["is_template"].lower() == "true" for rec in slist) > 1
    for rec in slist:
        rec2 = _cmd("sample", "info", rec["id"])
        rec3 = _cmd("sample", "info", rec["name"])
        assert rec == rec2 and rec == rec3


def test_sample_clone():
    cname = "NLP-API"
    pname = "testclone"
    rrec1 = _cmd("sample", "clone", cname, "--name", pname)
    with pytest.raises(CMDException) as excinfo:
        _cmd("sample", "clone", cname, "--name", pname)
    rrec2 = _cmd("sample", "clone", cname, "--name", pname, "--make-unique")
    rrec3 = _cmd("sample", "clone", cname)
    _cmd("project", "delete", rrec1["id"])
    _cmd("project", "delete", rrec2["id"])
    _cmd("project", "delete", rrec3["id"])


@pytest.fixture(scope="module")
def cli_project(project_list):
    return next(rec for rec in project_list if rec["name"] == "testproj3")


@pytest.fixture(scope="module")
def cli_revisions(cli_project):
    prec = cli_project
    revs = _cmd("project", "revision", "list", prec["id"])
    return prec, revs


@pytest.fixture(scope="module")
def downloaded_project(user_session, cli_revisions):
    prec, revs = cli_revisions
    with tempfile.TemporaryDirectory() as tempd:
        fname = _cmd("project", "download", prec["id"], table=False).strip()
        assert fname == prec["name"] + ".tar.gz"
        with tarfile.open(fname, "r") as tf:
            tf.extractall(path=tempd)
        dnames = glob.glob(os.path.join(tempd, "*", "anaconda-project.yml"))
        assert len(dnames) == 1
        dname = os.path.dirname(dnames[0])
        yield fname, dname
    for r in _cmd("project", "list"):
        if r["name"].startswith("test_upload"):
            _cmd("project", "delete", r["id"])
    assert not any(r["name"].startswith("test_upload") for r in _cmd("project", "list"))


def test_project_download(downloaded_project):
    pass


def test_project_upload(downloaded_project):
    fname, dname = downloaded_project
    _cmd("project", "upload", fname, "--name", "test_upload1", "--tag", "1.2.3")
    rrec = _cmd("project", "revision", "list", "test_upload1")
    assert len(rrec) == 1
    rev = rrec[0]["name"]
    fname2 = _cmd("project", "download", f"test_upload1:{rev}", table=False).strip()
    assert fname2 == f"test_upload1-{rev}.tar.gz"
    assert os.path.exists(fname2)
    _compare_tarfiles(fname, fname2)
    if rev == "0.0.1":
        pytest.xfail("5.4.1 revision issue")
    assert rev == "1.2.3"


def test_project_upload_as_directory(downloaded_project):
    """Behavior changes in 5.6.2"""
    fname, dname = downloaded_project
    _cmd("project", "upload", dname, "--name", "test_upload2", "--tag", "1.3.4")
    rrec = _cmd("project", "revision", "list", "test_upload2")
    assert len(rrec) == 1
    rev = rrec[0]["name"]
    fname2 = _cmd("project", "download", f"test_upload2:{rev}", table=False).strip()
    assert fname2 == f"test_upload2-{rev}.tar.gz"
    assert os.path.exists(fname2)
    assert rev == "1.3.4"


def test_project_revisions(cli_revisions):
    prec, revs = cli_revisions
    rev0 = _cmd("project", "revision", "info", prec["id"])
    assert revs[0] == rev0
    rev0 = _cmd("project", "revision", "info", prec["id"] + ":latest")
    assert revs[0] == rev0
    for rev in revs:
        revN = _cmd("project", "revision", "info", prec["id"] + ":" + rev["id"])
        assert rev == revN


def test_project_revision_errors(cli_revisions):
    prec, revs = cli_revisions
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "revision", "info", "testproj1")
    assert "Multiple projects" in str(excinfo.value)
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "revision", "info", "testproj4")
    assert "No projects" in str(excinfo.value)
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "revision", "info", prec["id"] + ":a.b.c")
    assert "No revisions" in str(excinfo.value)


def test_project_patch(cli_project, editors, resource_profiles):
    prec = cli_project
    old, new = {}, {}
    for what, wlist in (
        ("resource-profile", (r["name"] for r in resource_profiles)),
        ("editor", (e["id"] for e in editors)),
    ):
        old[what] = prec[what.replace("-", "_")]
        new[what] = next(v for v in wlist if v != old)
    cmd0 = ["project", "patch", prec["id"]]
    prec2 = _cmd(*(cmd0 + [f"--{k}={v}" for k, v in new.items()]))
    assert {k: prec2[k.replace("-", "_")] for k in new} == new
    prec3 = _cmd(*(cmd0 + [f"--{k}={v}" for k, v in old.items()]))
    assert {k: prec3[k.replace("-", "_")] for k in old} == old


def test_project_collaborators(cli_project, project_list):
    prec = cli_project
    uname = next(rec["owner"] for rec in project_list if rec["owner"] != prec["owner"])
    id = prec["id"]
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "collaborator", "info", id, uname)
    assert f"No collaborators found matching id={uname}" in str(excinfo.value)
    clist = _cmd("project", "collaborator", "add", id, uname)
    assert len(clist) == 1
    clist = _cmd("project", "collaborator", "add", id, "everyone", "--group", "--read-only")
    assert len(clist) == 2
    assert all(
        c["id"] == uname
        and c["permission"] == "rw"
        and c["type"] == "user"
        or c["id"] == "everyone"
        and c["permission"] == "r"
        and c["type"] == "group"
        for c in clist
    )
    clist = _cmd("project", "collaborator", "add", id, uname, "--read-only")
    assert len(clist) == 2
    assert all(
        c["id"] == uname
        and c["permission"] == "r"
        and c["type"] == "user"
        or c["id"] == "everyone"
        and c["permission"] == "r"
        and c["type"] == "group"
        for c in clist
    )
    clist = _cmd("project", "collaborator", "remove", id, uname, "everyone")
    assert len(clist) == 0
    with pytest.raises(CMDException) as excinfo:
        clist = _cmd("project", "collaborator", "remove", id, uname)
    assert f"Collaborator(s) not found: {uname}" in str(excinfo.value)


def test_project_activity(cli_project):
    prec = cli_project
    activity = _cmd("project", "activity", prec["id"])
    assert 1 <= len(activity) <= 10
    activity2 = _cmd("project", "activity", "--latest", prec["id"])
    assert activity[0] == activity2
    activity3 = _cmd("project", "activity", "--limit", "1", prec["id"])
    assert activity[0] == activity3[0]
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "activity", "--latest", "--all", prec["id"])
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "activity", "--limit", "2", "--all", prec["id"])
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "activity", "--latest", "--limit", "2", prec["id"])


@pytest.fixture(scope="module")
def cli_session(cli_project):
    prec = cli_project
    srec = _cmd("session", "start", f'{prec["owner"]}/{prec["name"]}')
    srec2 = _cmd("session", "restart", srec["id"], "--wait")
    assert not any(r["id"] == srec["id"] for r in _cmd("session", "list"))
    yield prec, srec2
    _cmd("session", "stop", srec2["id"])
    assert not any(r["id"] == srec2["id"] for r in _cmd("session", "list"))


def test_session(cli_session):
    prec, srec = cli_session
    assert srec["owner"] == prec["owner"], srec
    assert srec["name"] == prec["name"], srec
    # Ensure that the session can be retrieved by its project ID as well
    srec2 = _cmd("session", "info", f'{srec["owner"]}/*/{prec["id"]}')
    assert srec2["id"] == srec["id"]
    endpoint = srec["id"].rsplit("-", 1)[-1]
    sdata = _cmd("call", "/", f"--endpoint={endpoint}", table=False)
    assert "Jupyter Notebook requires JavaScript." in sdata, sdata


def test_project_sessions(cli_session):
    prec, srec = cli_session
    slist = _cmd("project", "sessions", prec["id"])
    assert len(slist) == 1 and slist[0]["id"] == srec["id"]


def test_session_branches_5_7_0(cli_session):
    """Behavior updated in 5.7.0"""
    prec, srec = cli_session
    branches = _cmd("session", "branches", prec["id"])
    bdict = {r["branch"]: r["sha1"] for r in branches}
    assert set(bdict) == {"master", "parent", "local"}, branches
    assert bdict["local"] == bdict["master"], branches


def test_session_before_changes(cli_session):
    prec, srec = cli_session
    changes1 = _cmd("session", "changes", prec["id"])
    changes1 = [c for c in changes1 if c["path"] != ".projectignore"]
    assert changes1 == [], changes1
    changes2 = _cmd("session", "changes", "--master", prec["id"])
    changes2 = [c for c in changes1 if c["path"] != ".projectignore"]
    assert changes2 == [], changes2


@pytest.fixture(scope="module")
def cli_deployment(cli_project):
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
    drec2 = _cmd("deployment", "restart", drec["id"], "--wait")
    assert not any(r["id"] == drec["id"] for r in _cmd("deployment", "list"))
    yield prec, drec2
    _cmd("deployment", "stop", drec2["id"])
    assert not any(r["id"] == drec2["id"] for r in _cmd("deployment", "list"))


def test_deploy(cli_deployment):
    prec, drec = cli_deployment
    assert drec["owner"] == prec["owner"], drec
    assert drec["project_name"] == prec["name"], drec
    for attempt in range(10):
        try:
            ldata = _cmd("call", "/", "--endpoint", drec["endpoint"], table=False)
            break
        except (AEUnexpectedResponseError, CMDException):
            time.sleep(attempt * 10)
            pass
    else:
        raise RuntimeError("Could not get the endpoint to respond")
    assert ldata.strip() == "Hello Anaconda Enterprise!", ldata


def test_project_deployments(cli_deployment):
    prec, drec = cli_deployment
    dlist = _cmd("project", "deployments", prec["id"])
    assert len(dlist) == 1 and dlist[0]["id"] == drec["id"]


def test_deploy_patch(cli_deployment):
    prec, drec = cli_deployment
    flag = "--private" if drec["public"].lower() == "true" else "--public"
    drec2 = _cmd("deployment", "patch", flag, drec["id"])
    assert drec2["public"] != drec["public"]
    flag = "--private" if drec2["public"].lower() == "true" else "--public"
    drec3 = _cmd("deployment", "patch", flag, drec["id"])
    assert drec3["public"] == drec["public"]


def test_deploy_token(user_session, cli_deployment):
    prec, drec = cli_deployment
    token = _cmd("deployment", "token", drec["id"], table=False).strip()
    resp = requests.get(
        f'https://{drec["endpoint"]}.' + user_session.hostname,
        headers={"Authorization": f"Bearer {token}"},
        verify=False,
    )
    assert resp.status_code == 200
    assert resp.text.strip() == "Hello Anaconda Enterprise!", resp.text


def test_deploy_logs(cli_deployment):
    prec, drec = cli_deployment
    id = drec["id"]
    app_prefix = "anaconda-app-" + id.rsplit("-", 1)[-1] + "-"
    app_logs = _cmd("deployment", "logs", id, table=False)
    event_logs = _cmd("deployment", "logs", id, "--events", table=False)
    proxy_logs = _cmd("deployment", "logs", id, "--proxy", table=False)
    assert "The project is ready to run commands." in app_logs
    assert app_prefix in event_logs, event_logs
    assert "App Proxy is fully operational!" in proxy_logs, proxy_logs


@pytest.mark.skip(reason="Failing against CI - Unable to reproduce in other environments")
def test_deploy_duplicate(cli_deployment):
    prec, drec = cli_deployment
    dname = drec["name"] + "-dup"
    with pytest.raises(CMDException) as excinfo:
        _cmd(
            "project",
            "deploy",
            prec["id"],
            "--name",
            dname,
            "--endpoint",
            drec["endpoint"],
            "--command",
            "default",
            "--private",
            "--wait",
        )
    assert f'endpoint "{drec["endpoint"]}" is already in use' in str(excinfo.value)
    assert not any(r["name"] == dname for r in _cmd("deployment", "list"))


def test_deploy_collaborators(cli_deployment):
    uname = "tooltest2"
    prec, drec = cli_deployment
    clist = _cmd("deployment", "collaborator", "list", drec["id"])
    assert len(clist) == 0
    clist = _cmd("deployment", "collaborator", "add", drec["id"], uname)
    assert len(clist) == 1
    clist = _cmd("deployment", "collaborator", "add", drec["id"], "everyone", "--group")
    assert len(clist) == 2
    clist = _cmd("deployment", "collaborator", "add", drec["id"], uname)
    assert len(clist) == 2
    assert all(
        c["id"] == uname and c["type"] == "user" or c["id"] == "everyone" and c["type"] == "group" for c in clist
    )
    for crec in clist:
        crec2 = _cmd("deployment", "collaborator", "info", drec["id"], crec["id"])
        assert crec2["id"] == crec["id"] and crec2["type"] == crec["type"]
    clist = _cmd("deployment", "collaborator", "remove", drec["id"], uname, "everyone")
    assert len(clist) == 0
    with pytest.raises(CMDException) as excinfo:
        clist = _cmd("deployment", "collaborator", "remove", drec["id"], uname)
    assert f"Collaborator(s) not found: {uname}" in str(excinfo.value)


def test_deploy_broken(cli_deployment):
    prec, drec = cli_deployment
    dname = drec["name"] + "-broken"
    with pytest.raises(CMDException) as excinfo:
        _cmd("project", "deploy", prec["id"], "--name", dname, "--command", "broken", "--private", "--stop-on-error")
    assert "Error completing deployment start: App failed to run" in str(excinfo.value)
    assert not any(r["name"] == dname for r in _cmd("deployment", "list"))


@pytest.mark.skip(reason="Failing against CI - k8s gravity issue")
def test_k8s_node(user_session):
    user_session.disconnect()
    nlist = _cmd("node", "list")
    for nrec in nlist:
        nrec2 = _cmd("node", "info", nrec["name"])
        assert nrec2["name"] == nrec["name"]


@pytest.mark.skip(reason="Failing against CI - k8s gravity issue")
def test_k8s_pod(user_session, cli_session, cli_deployment):
    _, srec = cli_session
    _, drec = cli_deployment
    plist = _cmd("pod", "list")
    assert any(prec["id"] == srec["id"] for prec in plist)
    assert any(prec["id"] == drec["id"] for prec in plist)
    for prec in plist:
        prec2 = _cmd("pod", "info", prec["id"])
        assert prec2["id"] == prec["id"]
    srec2 = _cmd("session", "info", srec["id"], "--k8s")
    assert srec2["id"] == srec["id"]
    drec2 = _cmd("deployment", "info", drec["id"], "--k8s")
    assert drec2["id"] == drec["id"]


def test_job_run1(cli_project):
    prec = cli_project
    _cmd("job", "create", prec["id"], "--name", "testjob1", "--command", "run", "--run", "--wait")
    jrecs = _cmd("job", "list")
    assert len(jrecs) == 1, jrecs
    rrecs = _cmd("run", "list")
    assert len(rrecs) == 1, rrecs
    ldata1 = _cmd("run", "log", rrecs[0]["id"], table=False)
    assert ldata1.strip().endswith("Hello Anaconda Enterprise!"), repr(ldata1)
    _cmd("job", "create", prec["id"], "--name", "testjob1", "--make-unique", "--command", "run", "--run", "--wait")
    jrecs = _cmd("job", "list")
    assert len(jrecs) == 2, jrecs
    jrecs2 = _cmd("project", "jobs", prec["id"])
    assert {r["id"]: r for r in jrecs} == {r["id"]: r for r in jrecs2}
    rrecs = _cmd("run", "list")
    assert len(rrecs) == 2, rrecs
    rrecs2 = _cmd("project", "runs", prec["id"])
    assert {r["id"]: r for r in rrecs} == {r["id"]: r for r in rrecs2}
    for rrec in rrecs:
        _cmd("run", "delete", rrec["id"])
    for jrec in jrecs:
        _cmd("job", "delete", jrec["id"])
    assert not _cmd("job", "list")
    assert not _cmd("run", "list")


def test_job_run2(cli_project):
    prec = cli_project
    # Test cleanup mode and variables in jobs
    variables = {"INTEGRATION_TEST_KEY_1": "value1", "INTEGRATION_TEST_KEY_2": "value2"}
    cmd = ["project", "run", prec["id"], "--command", "run_with_env_vars", "--name", "testjob2"]
    for k, v in variables.items():
        cmd.extend(("--variable", f"{k}={v}"))
    _cmd(*cmd)
    # The job record should have already been deleted
    assert not _cmd("job", "list")
    rrecs = _cmd("run", "list")
    assert len(rrecs) == 0, rrecs


def test_login_time(admin_session, user_session):
    # The current login time should be before the present
    now = datetime.now()
    user_list = _cmd("user", "list")
    urec = next((r for r in user_list if r["username"] == user_session.username), None)
    assert urec is not None
    ltm1 = datetime.strptime(urec["lastLogin"], "%Y-%m-%d %H:%M:%S.%f")
    assert ltm1 < now
    # No more testing here, because we want to preserve the existing sessions


@pytest.mark.skip(reason="Failing Against 5.7.0 Due To KeyCloack Changes")
def test_realm_roles(admin_session):
    _cmd("project", "list")
    user_list = _cmd("user", "list")

    # Validate realms roles are present on the user
    assert "realm_roles" in user_list[0]
