import os
import pytest

from ae5_tools.api import AEUserSession, AEAdminSession


def _get_vars(*vars):
    missing = [v for v in vars if not os.environ.get(v)]
    if missing:
        raise RuntimeError('The following environment variables must be set: {}'.format(' '.join(missing)))
    result = tuple(os.environ[v] for v in vars)
    return result[0] if len(result) == 1 else result


# Expectations: the user AE5_USERNAME should have at least three projects:
# - project names: testproj1, testproj2, testproj3
# - all three editors should be represented
# - the projects should have 0, 1, and 2 collaborators
@pytest.fixture
def user_setup():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_USERNAME', 'AE5_PASSWORD')
    s = AEUserSession(hostname, username, password)
    for run in s.run_list():
        s.run_delete(run['id'])
    for job in s.job_list():
        s.job_delete(job['id'])
    for dep in s.deployment_list():
        s.deployment_stop(dep['id'])
    for sess in s.session_list():
        s.session_stop(sess['id'])
    plist = s.project_list(collaborators=True)
    for p in plist:
        if p['name'] not in {'testproj1', 'testproj2', 'testproj3'}:
            s.project_delete(p['id'])
    if set(p['name'] for p in plist) != {'testproj1', 'testproj2', 'testproj3'}:
        raise RuntimeErrror('Test account has not been properly prepared')
    plist = [p for p in plist if p['name'] in {'testproj1', 'testproj2', 'testproj3'}]
    assert len(plist) == 3
    assert all(p['owner'] == username for p in plist)
    editors = set(p['editor'] for p in plist)
    assert editors == {'notebook', 'jupyterlab', 'zeppelin'}
    collabs = set(len(p['collaborators'].split(', ')) if p['collaborators'] else 0
                  for p in plist)
    assert collabs == {0, 1, 2}
    yield s, plist
    s.disconnect()


@pytest.fixture
def admin_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_ADMIN_USERNAME', 'AE5_ADMIN_PASSWORD')
    s = AEAdminSession(hostname, username, password)
    yield s
    s.disconnect()


@pytest.fixture()
def user_session(user_setup):
    return user_setup[0]


@pytest.fixture()
def project_list(user_setup):
    return user_setup[1]



