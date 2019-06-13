import pytest

import tempfile
import os

from ae5_tools.api import AEUserSession, AEAdminSession


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
    return admin_session.impersonate(username)


@pytest.fixture()
def user_project_list(user_session):
    return user_session.project_list(collaborators=True)


@pytest.fixture()
def user_project_list_imp(impersonate_session):
    return impersonate_session.project_list(collaborators=True)


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
        collab2 = user_session.project_collaborators(rec0['id'])
        collab3 = set(c['id'] for c in collab2)
        assert collabs == collab3, collab2


def test_project_activity(user_session, project_set):
    for rec0 in project_set:
        activity = user_session.project_activity(rec0['id'])
        assert all(rec0['owner'] == rec1['owner'] for rec1 in activity)
        assert activity[-1]['type'] == 'create_action' and activity[-1]['done']


def test_project_download_upload_delete(user_session, project_set, user_project_list):
    assert not any(r['name'] == 'test_upload' for r in user_project_list)
    with tempfile.TemporaryDirectory() as tempd:
        fname = os.path.join(tempd, 'blob')
        user_session.project_download(project_set[0]['id'], filename=fname)
        user_session.project_upload(fname, 'test_upload', '1.2.3', wait=True)
        for r in user_session.project_list():
            if r['name'] == 'test_upload':
                user_session.project_delete(r['id'])
                break
        else:
            assert False, 'Uploaded project could not be found'
    assert not any(r['name'] == 'test_upload' for r in user_session.project_list())
