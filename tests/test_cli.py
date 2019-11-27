import pytest

import tempfile
import time
import os
import pytest
import pprint
import requests
import tarfile
import glob
import uuid

from datetime import datetime
from collections import namedtuple
from ae5_tools.api import AEUnexpectedResponseError
from subprocess import CalledProcessError

from .utils import _cmd


Session = namedtuple('Session', 'hostname username')


@pytest.fixture
def project_list_cli(user_session):
    return _cmd('project list --collaborators')


def test_project_info(project_list_cli):
    for rec0 in project_list_cli:
        id = rec0['id']
        pair = '{}/{}'.format(rec0['owner'], rec0['name'])
        rec1 = _cmd(f'project info {id}')
        rec2 = _cmd(f'project info {pair}')
        rec3 = _cmd(f'project info {pair}/{id}')
        assert all(rec0[k] == v for k, v in rec2.items()), pprint.pformat((rec0, rec2))
        assert all(rec1[k] == v for k, v in rec2.items()), pprint.pformat((rec1, rec2))
        assert rec2 == rec3


def test_project_collaborators(project_list_cli):
    for rec0 in project_list_cli:
        collabs = rec0['collaborators']
        collabs = set(collabs.split(', ')) if collabs else set()
        collab2 = _cmd(f'project collaborator list {rec0["id"]}')
        collab3 = set(c['id'] for c in collab2)
        assert collabs == collab3, collab2


def test_project_activity(project_list_cli):
    for rec0 in project_list_cli:
        activity = _cmd(f'project activity --limit -1 {rec0["owner"]}/{rec0["name"]}')
        assert activity[-1]['status'] == 'created'
        assert activity[-1]['done'] == 'True'
        assert activity[-1]['owner'] == rec0['owner']


@pytest.fixture(scope='module')
def downloaded_project(user_session):
    uname = user_session.username
    with tempfile.TemporaryDirectory() as tempd:
        fname = os.path.join(tempd, 'blob.tar.gz')
        fname2 = os.path.join(tempd, 'blob2.tar.gz')
        _cmd(f'project download {uname}/testproj1 --filename {fname}', table=False)
        with tarfile.open(fname, 'r') as tf:
            tf.extractall(path=tempd)
        dnames = glob.glob(os.path.join(tempd, '*', 'anaconda-project.yml'))
        assert len(dnames) == 1
        dname = os.path.dirname(dnames[0])
        yield fname, fname2, dname
    for r in user_session.project_list():
        if r['owner'] == uname and r['name'].startswith('test_upload'):
            _cmd(f'project delete {r["id"]} --yes', table=False)
    assert not any(r['owner'] == uname and r['name'].startswith('test_upload')
                   for r in _cmd('project list'))


def test_project_download(downloaded_project):
    pass


def test_project_upload(user_session, downloaded_project):
    fname, fname2, dname = downloaded_project
    _cmd(f'project upload {fname} --name test_upload1 --tag 1.2.3')
    rrec = _cmd(f'project revision list test_upload1')
    assert len(rrec) == 1
    assert rrec[0]['name'] == '1.2.3'
    _cmd(f'project download test_upload1 --filename {fname2}', table=False)


def test_project_upload_as_directory(user_session, downloaded_project):
    fname, fname2, dname = downloaded_project
    _cmd(f'project upload {dname} --name test_upload2 --tag 1.2.3')
    rrec = _cmd(f'project revision list test_upload2')
    assert len(rrec) == 1
    assert rrec[0]['name'] == '1.2.3'
    _cmd(f'project download test_upload2 --filename {fname2}', table=False)


def test_project_create_from_sample(user_session):
    uname = user_session.username
    cname = 'nlp_api'
    pname = 'testclone'
    rrec = _cmd(f'sample clone {cname} --name {pname}')
    _cmd(f'project delete {rrec["id"]} --yes', table=False)


def test_job_run1(user_session):
    uname = user_session.username
    _cmd(f'job create {uname}/testproj3 --name testjob1 --command run --run --wait')
    jrecs = _cmd('job list')
    assert len(jrecs) == 1, jrecs
    rrecs = _cmd('run list')
    assert len(rrecs) == 1, rrecs
    ldata1 = _cmd(f'run log {rrecs[0]["id"]}', table=False)
    assert ldata1.strip().endswith('Hello Anaconda Enterprise!'), repr(ldata1)
    _cmd(f'job create {uname}/testproj3 --name testjob1 --make-unique --command run --run --wait')
    jrecs = _cmd('job list')
    assert len(jrecs) == 2, jrecs
    rrecs = _cmd('run list')
    assert len(rrecs) == 2, rrecs
    for rrec in rrecs:
        _cmd(f'run delete {rrec["id"]} --yes', table=False)
    for jrec in jrecs:
        _cmd(f'job delete {jrec["id"]} --yes', table=False)
    assert not _cmd('job list')
    assert not _cmd('run list')


def test_job_run2(user_session):
    uname = user_session.username
    # Test cleanup mode and variables in jobs
    variables = {'INTEGRATION_TEST_KEY_1': 'value1', 'INTEGRATION_TEST_KEY_2': 'value2'}
    vars = ' '.join(f'--variable {k}={v}' for k, v in variables.items())
    _cmd(f'project run {uname}/testproj3 --command run_with_env_vars --name testjob2 {vars}')
    # The job record should have already been deleted
    assert not _cmd('job list')
    rrecs = _cmd('run list')
    assert len(rrecs) == 1, rrecs
    ldata2 = _cmd(f'run log {rrecs[0]["id"]}', table=False)
    # Confirm that the environment variables were passed through
    outvars = dict(line.strip().replace(' ', '').split(':', 1)
                   for line in ldata2.splitlines()
                   if line.startswith('INTEGRATION_TEST_KEY_'))
    assert variables == outvars, outvars
    _cmd(f'run delete {rrecs[0]["id"]} --yes', table=False)
    assert not _cmd('run list')


@pytest.fixture(scope='module')
def cli_session(user_session):
    uname = user_session.username
    pname = 'testproj3'
    _cmd(f'session start {uname}/{pname} --wait', table=False)
    srecs = [r for r in _cmd('session list')
             if r['owner'] == uname and r['name'] == pname]
    assert len(srecs) == 1, srecs
    yield srecs[0]['id'], pname
    _cmd(f'session stop {uname}/{pname} --yes', table=False)
    srecs = [r for r in _cmd('session list')
             if r['owner'] == uname and r['name'] == dname
             or r['id'] == srecs[0]['id']]
    assert len(srecs) == 0, srecs


def test_session(user_session, cli_session):
    id, pname = cli_session
    endpoint = id.rsplit("-", 1)[-1]
    sdata = _cmd(f'call / --endpoint={endpoint}', table=False)
    assert 'Jupyter Notebook requires JavaScript.' in sdata, sdata


def test_session_branches(user_session, cli_session):
    id, pname = cli_session
    branches = _cmd(f'session branches {id}')
    bdict = {r['branch']: r['sha1'] for r in branches}
    assert set(bdict) == {'local', 'origin/local', 'master'}, branches
    assert bdict['local'] == bdict['master'], branches


def test_session_before_changes(user_session, cli_session):
    id, pname = cli_session
    changes1 = _cmd(f'session changes {id}')
    assert changes1 == [], changes1
    changes2 = _cmd(f'session changes --master {id}')
    assert changes2 == [], changes2


@pytest.fixture(scope='module')
def cli_deployment(user_session):
    uname = user_session.username
    dname = 'testdeploy'
    ename = 'testendpoint'
    _cmd(f'project deploy {uname}/testproj3 --name {dname} --endpoint {ename} --command default --private --wait', table=False)
    drecs = [r for r in _cmd('deployment list')
             if r['owner'] == uname and r['name'] == dname]
    assert len(drecs) == 1, drecs
    yield drecs[0]['id'], ename
    _cmd(f'deployment stop {drecs[0]["id"]} --yes', table=False)
    drecs = [r for r in _cmd('deployment list')
             if r['owner'] == uname and r['name'] == dname
             or r['id'] == drecs[0]['id']]
    assert len(drecs) == 0, drecs


def test_deploy(user_session, cli_deployment):
    id, ename = cli_deployment
    for attempt in range(3):
        try:
            ldata = _cmd(f'call / --endpoint {ename}', table=False)
            break
        except AEUnexpectedResponseError:
            time.sleep(attempt * 5)
            pass
    else:
        raise RuntimeError("Could not get the endpoint to respond")
    assert ldata.strip() == 'Hello Anaconda Enterprise!', ldata


def test_deploy_token(user_session, cli_deployment):
    id, ename = cli_deployment
    token = _cmd(f'deployment token {id}', table=False).strip()
    resp = requests.get(f'https://{ename}.' + user_session.hostname,
                        headers={'Authorization': f'Bearer {token}'})
    assert resp.status_code == 200
    assert resp.text.strip() == 'Hello Anaconda Enterprise!', resp.text


def test_deploy_logs(user_session, cli_deployment):
    id, ename = cli_deployment
    app_prefix = 'anaconda-app-' + id.rsplit("-", 1)[-1] + '-'
    app_logs = _cmd(f'deployment logs {id}', table=False)
    event_logs = _cmd(f'deployment logs {id} --events', table=False)
    proxy_logs = _cmd(f'deployment logs {id} --proxy', table=False)
    assert 'The project is ready to run commands.' in app_logs
    assert app_prefix in event_logs, event_logs
    assert 'App Proxy is fully operational!' in proxy_logs, proxy_logs


def test_deploy_duplicate(user_session, cli_deployment):
    uname = user_session.username
    id, ename = cli_deployment
    dname = 'testdeploy2'
    with pytest.raises(CalledProcessError):
        _cmd(f'project deploy {uname}/testproj3 --name {dname} --endpoint {ename} --command default --private --wait', table=False)
    drecs = [r for r in _cmd('deployment list')
             if r['owner'] == uname and r['name'] == dname]
    assert len(drecs) == 0, drecs


def test_deploy_broken(user_session):
    uname = user_session.username
    dname = 'testbroken'
    with pytest.raises(CalledProcessError):
        _cmd(f'project deploy {uname}/testproj3 --name {dname} --command broken --private --stop-on-error', table=False)
    drecs = [r for r in _cmd('deployment list')
             if r['owner'] == uname and r['name'] == dname]
    assert len(drecs) == 0, drecs


def test_login_time(admin_session, user_session):
    # The current login time should be before the present
    now = datetime.utcnow()
    _cmd('project list')
    user_list = _cmd('user list')
    urec = next((r for r in user_list if r['username'] == user_session.username), None)
    assert urec is not None
    ltm1 = datetime.strptime(urec['lastLogin'], "%Y-%m-%d %H:%M:%S.%f")
    assert ltm1 < now
    # No more testing here, because we want to preserve the existing sessions
