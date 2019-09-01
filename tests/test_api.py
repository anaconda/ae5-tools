import pytest

import tempfile
import time
import os

from datetime import datetime

from ae5_tools.api import AEUserSession, AEAdminSession, AEUnexpectedResponseError


def _get_vars(*vars):
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        raise RuntimeError('The following environment variables must be set: {}'.format(' '.join(missing)))
    result = tuple(os.environ[v] for v in vars)
    return result[0] if len(result) == 1 else result


@pytest.fixture
def user_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_USERNAME', 'AE5_PASSWORD')
    return AEUserSession(hostname, username, password)


@pytest.fixture
def admin_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_ADMIN_USERNAME', 'AE5_ADMIN_PASSWORD')
    return AEAdminSession(hostname, username, password)


@pytest.fixture
def impersonate_session(admin_session):
    username = _get_vars('AE5_USERNAME')
    return AEUserSession(admin_session.hostname, username, admin_session)


@pytest.fixture()
def user_project_list(user_session):
    project_list = user_session.project_list(collaborators=True)
    for r in project_list:
        if r['name'] == 'test_upload':
            user_session.project_delete(r['id'])
    return [r for r in project_list if r['name'] != 'test_upload']


@pytest.fixture()
def user_run_list(user_session):
    job_list = user_session.run_list()
    for r in job_list:
        if r['name'].startswith('testjob'):
            user_session.run_delete(r['id'])
    return [r for r in job_list if not r['name'].startswith('testjob')]


@pytest.fixture()
def user_job_list(user_session, user_run_list):
    job_list = user_session.job_list()
    for r in job_list:
        if r['name'].startswith('testjob'):
            user_session.job_delete(r['id'])
    return [r for r in job_list if not r['name'].startswith('testjob')]


@pytest.fixture()
def user_deploy_list(user_session):
    deploy_list = user_session.deployment_list()
    for r in deploy_list:
        if r['name'] == 'testdeploy':
            user_session.deployment_stop(r['id'])
    return [r for r in deploy_list if not r['name'] == 'testdeploy']


@pytest.fixture()
def user_project_list_imp(impersonate_session):
    project_list = impersonate_session.project_list(collaborators=True)
    for r in project_list:
        if r['name'] == 'test_upload':
            impersonate_session.project_delete(r['id'])
    return [r for r in project_list if r['name'] != 'test_upload']


@pytest.fixture()
def project_set(user_project_list):
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


def test_project_list_imp(project_set, user_project_list_imp):
    for r1 in project_set:
        assert any(r1 == r2 for r2 in user_project_list_imp)


def test_project_info(user_session, project_set):
    for rec0 in project_set:
        id = rec0['id']
        pair = '{}/{}'.format(rec0['owner'], rec0['name'])
        rec1 = user_session.project_info(id, collaborators=True)
        rec2 = user_session.project_info(pair)
        rec3 = user_session.project_info('{}/{}'.format(pair, id))
        assert all(rec0[k] == v for k, v in rec2.items())
        assert all(rec1[k] == v for k, v in rec2.items())
        assert rec2 == rec3


def test_project_collaborators(user_session, project_set):
    for rec0 in project_set:
        collabs = rec0['collaborators']
        collabs = set(collabs.split(', ')) if collabs else set()
        collab2 = user_session.project_collaborator_list(rec0['id'])
        collab3 = set(c['id'] for c in collab2)
        assert collabs == collab3, collab2


def test_project_activity(user_session, project_set):
    activity = user_session.project_activity('testproj3')
    assert activity[-1]['done']


def test_project_download_upload_delete(user_session, project_set, user_project_list):
    assert not any(r['name'] == 'test_upload' for r in user_project_list)
    with tempfile.TemporaryDirectory() as tempd:
        fname = os.path.join(tempd, 'blob')
        fname2 = os.path.join(tempd, 'blob2')
        user_session.project_download(project_set[0]['id'], filename=fname)
        prec = user_session.project_upload(fname, 'test_upload', '1.2.3', wait=True)
        user_session.project_download(prec['id'], filename=fname2)
        for r in user_session.project_list():
            if r['name'] == 'test_upload':
                user_session.project_delete(r['id'])
                break
        else:
            assert False, 'Uploaded project could not be found'
    assert not any(r['name'] == 'test_upload' for r in user_session.project_list())


def test_job_run1(user_session, user_job_list, user_run_list):
    user_session.job_create('testproj3', name='testjob1', command='run', run=True, wait=True)
    jrecs = [r for r in user_session.job_list(format='json') if r['name'] == 'testjob1']
    assert len(jrecs) == 1, jrecs
    rrecs = [r for r in user_session.run_list(format='json') if r['name'] == 'testjob1']
    assert len(rrecs) == 1, rrecs
    ldata1 = user_session.run_log(rrecs[0]['id'], format='text')
    assert ldata1.endswith('Hello Anaconda Enterprise!\n'), repr(ldata1)
    user_session.run_delete(rrecs[0]['id'])
    user_session.job_delete(jrecs[0]['id'])
    assert not any(r['name'] != 'testjob1' for r in user_session.job_list())
    assert not any(r['name'] != 'testjob1' for r in user_session.run_list())


def test_job_run2(user_session, user_job_list, user_run_list):
    # Test cleanup mode and variables in jobs
    variables = {'INTEGRATION_TEST_KEY_1': 'value1', 'INTEGRATION_TEST_KEY_2': 'value2'}
    user_session.job_create('testproj3', name='testjob2', command='run_with_env_vars',
                            variables=variables, run=True, wait=True, cleanup=True)
    # The job record should have already been deleted
    assert not any(r['name'] == 'testjob2' for r in user_session.job_list())
    rrecs = [r for r in user_session.run_list(format='json') if r['name'] == 'testjob2']
    assert len(rrecs) == 1, rrecs
    ldata2 = user_session.run_log(rrecs[0]['id'], format='text')
    # Confirm that the environment variables were passed through
    outvars = dict(line.strip().replace(' ', '').split(':', 1)
                   for line in ldata2.splitlines()
                   if line.startswith('INTEGRATION_TEST_KEY_'))
    assert variables == outvars, outvars
    user_session.run_delete(rrecs[0]['id'])
    assert not any(r['name'] == 'testjob2' for r in user_session.run_list())


def test_deploy(user_session, user_deploy_list):
    assert not any(r['name'] == 'testdeploy' for r in user_session.deployment_list())
    user_session.deployment_start('testproj3', name='testdeploy', endpoint='testendpoint',
                                  command='default', public=False, wait=True)
    drecs = [r for r in user_session.deployment_list(format='json') if r['name'] == 'testdeploy']
    assert len(drecs) == 1, drecs
    for attempt in range(3):
        try:
            ldata = user_session._get('/', subdomain='testendpoint', format='text')
            break
        except AEUnexpectedResponseError:
            time.sleep(attempt * 5)
            pass
    else:
        raise RuntimeError("Could not get the endpoint to respond")
    assert ldata.strip() == 'Hello Anaconda Enterprise!', ldata
    user_session.deployment_stop(drecs[0]['id'])
    assert not any(r['name'] == 'testdeploy' for r in user_session.deployment_list())


def test_login_time(admin_session, user_session):
    user_list = admin_session.user_list(format='json')
    urec = next((r for r in user_list if r['username'] == user_session.username), None)
    assert urec is not None
    ltm1 = urec['lastLogin']
    now = datetime.utcnow()
    # The last login time should be before the present
    assert ltm1 < now
    user_session.disconnect()
    imp_session = AEUserSession(admin_session.hostname, user_session.username, admin_session)
    plist1 = imp_session.project_list()
    urec = admin_session.user_info(urec['id'], format='json')
    ltm2 = urec['lastLogin']
    # The impersonation login should not affect the login time
    assert ltm1 == ltm2
    imp_session.disconnect()
    plist2 = user_session.project_list()
    urec = admin_session.user_info(urec['id'], format='json')
    ltm3 = urec['lastLogin']
    # The second login should come after the first
    assert ltm3 > ltm2
    # Confirm the impersonation worked by checking the project lists are the same
    assert plist1 == plist2

