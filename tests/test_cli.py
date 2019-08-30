import pytest

import tempfile
import subprocess
import time
import pandas
import os
import json
import shlex
import pandas as pd

from io import BytesIO
from datetime import datetime
from collections import namedtuple

from ae5_tools.api import AEUserSession, AEAdminSession, AEUnexpectedResponseError

Session = namedtuple('Session', 'hostname username')


def _get_vars(*vars):
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        raise RuntimeError('The following environment variables must be set: {}'.format(' '.join(missing)))
    result = tuple(os.environ[v] for v in vars)
    return result[0] if len(result) == 1 else result


def _cmd(cmd, table=True):
    # We go through Pandas to CSV to JSON instead of directly to JSON to improve coverage
    cmd = 'ae5 ' + cmd
    if table:
        cmd += f' --format csv'
    print(f'Executing: {cmd}')
    text = subprocess.check_output(shlex.split(cmd), stdin=open(os.devnull))
    if not table or not text.strip():
        return text.decode()
    csv = pd.read_csv(BytesIO(text)).fillna('').astype(str)
    if tuple(csv.columns) == ('field', 'value'):
        return csv.set_index('field').T.iloc[0].to_dict()
    return json.loads(csv.to_json(index=False, orient='table'))['data']


@pytest.fixture
def user_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_USERNAME', 'AE5_PASSWORD')
    return Session(hostname, username)


@pytest.fixture
def admin_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_ADMIN_USERNAME', 'AE5_ADMIN_PASSWORD')
    return Session(hostname, username)


@pytest.fixture()
def user_project_list(user_session):
    project_list = _cmd('project list --collaborators')
    for r in project_list:
        if r['name'] == 'test_upload':
            _cmd(f'project delete {r["id"]} --yes', table=False)
    return [r for r in project_list if r['name'] != 'test_upload']


@pytest.fixture()
def user_run_list(user_session):
    job_list = _cmd('run list')
    for r in job_list:
        if r['name'].startswith('testjob'):
            _cmd(f'run delete {r["id"]} --yes', table=False)
    return [r for r in job_list if not r['name'].startswith('testjob')]


@pytest.fixture()
def user_job_list(user_session, user_run_list):
    job_list = _cmd('job list')
    for r in job_list:
        if r['name'].startswith('testjob'):
            _cmd(f'job delete {r["id"]} --yes', table=False)
    return [r for r in job_list if not r['name'].startswith('testjob')]


@pytest.fixture()
def user_deploy_list(user_session):
    deploy_list = _cmd('deployment list')
    for r in deploy_list:
        if r['name'] == 'testdeploy':
            _cmd(f'deployment stop {r["id"]} --yes', table=False)
    return [r for r in deploy_list if not r['name'] == 'testdeploy']


@pytest.fixture()
def project_set(user_session, user_project_list):
    return [r for r in user_project_list if r['name'] in {'testproj1', 'testproj2', 'testproj3'}]


# Expectations: the user AE5_USERNAME should have at least three projects:
# - project names: testproj1, testproj2, testproj3
# - all three editors should be represented
# - the projects should have 0, 1, and 2 collaborators
# - there is no project named 'test_upload'
def test_project_list(user_session, project_set):
    assert len(project_set) == 3
    assert all(r['owner'] == user_session.username for r in project_set)
    editors = set(r['editor'] for r in project_set)
    assert editors == {'notebook', 'jupyterlab', 'zeppelin'}
    collabs = set(len(r['collaborators'].split(', ')) if r['collaborators'] else 0
                  for r in project_set)
    assert collabs == {0, 1, 2}


def test_project_info(user_session, project_set):
    for rec0 in project_set:
        id = rec0['id']
        pair = '{}/{}'.format(rec0['owner'], rec0['name'])
        rec1 = _cmd(f'project info {id}')
        rec2 = _cmd(f'project info {pair}')
        rec3 = _cmd(f'project info {pair}/{id}')
        assert all(rec0[k] == v for k, v in rec2.items()), (rec0, rec2)
        assert all(rec1[k] == v for k, v in rec2.items()), (rec1, rec2)
        assert rec2 == rec3


def test_project_collaborators(user_session, project_set):
    for rec0 in project_set:
        collabs = rec0['collaborators']
        collabs = set(collabs.split(', ')) if collabs else set()
        collab2 = _cmd(f'project collaborator list {rec0["id"]}')
        collab3 = set(c['id'] for c in collab2)
        assert collabs == collab3, collab2


def test_project_activity(user_session, project_set):
    for rec0 in project_set:
        activity = _cmd('project activity testproj3')
        assert all(rec0['owner'] == rec1['owner'] for rec1 in activity)
        assert activity[-1]['type'] == 'create_action' and activity[-1]['done']


def test_project_download_upload_delete(user_session, project_set, user_project_list):
    assert not any(r['name'] == 'test_upload' for r in user_project_list)
    with tempfile.TemporaryDirectory() as tempd:
        fname = os.path.join(tempd, 'blob')
        fname2 = os.path.join(tempd, 'blob2')
        _cmd(f'project download {project_set[0]["id"]} --filename {fname}', table=False)
        prec = _cmd(f'project upload {fname} --name test_upload --tag 1.2.3')
        _cmd(f'project download {prec["id"]} --filename {fname2}', table=False)
        for r in _cmd('project list'):
            if r['name'] == 'test_upload':
                _cmd(f'project delete {r["id"]} --yes', table=False)
                break
        else:
            assert False, 'Uploaded project could not be found'
    assert not any(r['name'] == 'test_upload' for r in _cmd('project list'))


def test_job_run1(user_session, user_job_list, user_run_list):
    _cmd('job create testproj3 --name testjob1 --command run --run --wait')
    jrecs = [r for r in _cmd('job list') if r['name'] == 'testjob1']
    assert len(jrecs) == 1, jrecs
    rrecs = [r for r in _cmd('run list') if r['name'] == 'testjob1']
    assert len(rrecs) == 1, rrecs
    ldata1 = _cmd(f'run log {rrecs[0]["id"]}', table=False)
    assert ldata1.strip().endswith('Hello Anaconda Enterprise!'), repr(ldata1)
    _cmd(f'run delete {rrecs[0]["id"]} --yes', table=False)
    _cmd(f'job delete {jrecs[0]["id"]} --yes', table=False)
    assert not any(r['name'] != 'testjob1' for r in _cmd('job list'))
    assert not any(r['name'] != 'testjob1' for r in _cmd('run list'))


def test_job_run2(user_session, user_job_list, user_run_list):
    # Test cleanup mode and variables in jobs
    variables = {'INTEGRATION_TEST_KEY_1': 'value1', 'INTEGRATION_TEST_KEY_2': 'value2'}
    vars = ' '.join(f'--variable {k}={v}' for k, v in variables.items())
    _cmd('project run testproj3 --command run_with_env_vars --name testjob2 ' + vars)
    # The job record should have already been deleted
    assert not any(r['name'] == 'testjob2' for r in _cmd('job list'))
    rrecs = [r for r in _cmd('run list') if r['name'] == 'testjob2']
    assert len(rrecs) == 1, rrecs
    ldata2 = _cmd(f'run log {rrecs[0]["id"]}', table=False)
    # Confirm that the environment variables were passed through
    outvars = dict(line.strip().replace(' ', '').split(':', 1)
                   for line in ldata2.splitlines()
                   if line.startswith('INTEGRATION_TEST_KEY_'))
    assert variables == outvars, outvars
    _cmd(f'run delete {rrecs[0]["id"]} --yes', table=False)
    assert not any(r['name'] == 'testjob2' for r in _cmd('run list'))


def test_deploy(user_session, user_deploy_list):
    assert not any(r['name'] == 'testdeploy' for r in _cmd('deployment list'))
    _cmd('project deploy testproj3 --name testdeploy --endpoint testendpoint --command default --private --wait --no-open', table=False)
    drecs = [r for r in _cmd('deployment list') if r['name'] == 'testdeploy']
    assert len(drecs) == 1, drecs
    for attempt in range(3):
        try:
            ldata = _cmd('call / --endpoint testendpoint', table=False)
            break
        except AEUnexpectedResponseError:
            time.sleep(attempt * 5)
            pass
    else:
        raise RuntimeError("Could not get the endpoint to respond")
    assert ldata.strip() == 'Hello Anaconda Enterprise!', ldata
    _cmd(f'deployment stop {drecs[0]["id"]} --yes', table=False)
    assert not any(r['name'] == 'testdeploy' for r in _cmd('deployment list'))


def test_login_time(admin_session, user_session):
    user_list = _cmd('user list')
    urec = next((r for r in user_list if r['username'] == user_session.username), None)
    assert urec is not None
    now = datetime.utcnow()
    assert datetime.fromisoformat(urec['lastLogin']) < now
    _cmd('logout', table=False)
    _cmd('login', table=False)
    urec = _cmd(f'user info {urec["id"]}')
    assert datetime.fromisoformat(urec['lastLogin']) > now


