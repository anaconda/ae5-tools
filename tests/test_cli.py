import pytest

import tempfile
import subprocess
import time
import pandas
import os
import json
import shlex
import pandas as pd
import pprint

from io import BytesIO
from datetime import datetime
from collections import namedtuple
from ae5_tools.api import AEUnexpectedResponseError


Session = namedtuple('Session', 'hostname username')


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


def test_project_activity(user_session):
    activity = _cmd('project activity testproj3')
    assert activity[-1]['done']


def test_project_download_upload_delete(user_session):
    with tempfile.TemporaryDirectory() as tempd:
        fname = os.path.join(tempd, 'blob')
        fname2 = os.path.join(tempd, 'blob2')
        _cmd(f'project download testproj1 --filename {fname}', table=False)
        prec = _cmd(f'project upload {fname} --name test_upload --tag 1.2.3')
        rrec = _cmd(f'project revision list test_upload')
        assert len(rrec) == 1
        assert rrec[0]['name'] == '1.2.3'
        _cmd(f'project download test_upload --filename {fname2}', table=False)
        for r in _cmd('project list'):
            if r['name'] == 'test_upload':
                _cmd(f'project delete {r["id"]} --yes', table=False)
                break
        else:
            assert False, 'Uploaded project could not be found'
    assert not any(r['name'] == 'test_upload' for r in _cmd('project list'))


def test_job_run1(user_session):
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


def test_job_run2(user_session):
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


def test_deploy(user_session):
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
    # The current login time should be before the present
    now = datetime.utcnow()
    prrec = _cmd('project list')
    user_list = _cmd('user list')
    urec = next((r for r in user_list if r['username'] == user_session.username), None)
    assert urec is not None
    ltm1 = datetime.strptime(urec['lastLogin'], "%Y-%m-%d %H:%M:%S.%f")
    assert ltm1 < now
    # No more testing here, because we want to preserve the existing sessions
