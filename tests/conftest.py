import pytest

from ae5_tools.api import AEUserSession, AEAdminSession

from .utils import _get_vars, _cmd


# Expectations: the user AE5_USERNAME should have at least three projects:
# - project names: testproj1, testproj2, testproj3
# - all three editors should be represented
# - the projects should have 0, 1, and 2 collaborators
# Furthermore, there should be a second user satisfying the following:
# - At least one project shared with this user as a collaborator
# - At least two of those projects are called testproj1, testproj2, or testproj3
@pytest.fixture(scope='session')
def user_setup():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_USERNAME', 'AE5_PASSWORD')
    s = AEUserSession(hostname, username, password)
    for run in s.run_list():
        s.run_delete(run)
    for job in s.job_list():
        s.job_delete(job)
    for dep in s.deployment_list():
        s.deployment_stop(dep)
    for sess in s.session_list():
        s.session_stop(sess)
    plist = s.project_list()
    for p in plist:
        if p['name'] not in {'testproj1', 'testproj2', 'testproj3'} and p['owner'] == username:
            s.project_delete(p['id'])
    plist = s.project_list(collaborators=True)
    powned = [p for p in plist if p['owner'] == username]
    pother = [p for p in plist if p['owner'] != username]
    # Assert there are exactly 3 projects owned by the test user
    assert len(powned) == 3
    # Need at least two duplicated project names to properly test sorting/filtering
    assert len(set(p['name'] for p in powned).intersection(p['name'] for p in pother)) >= 2
    # Make sure all three editors are represented
    assert len(set(p['editor'] for p in powned)) == 3
    # Make sure testproj3 is using the Jupyter editor
    assert any(p['editor'] == 'notebook' and p['name'] == 'testproj3' for p in powned)
    # Make sure we have 0, 1, and 2 collaborators represented
    prec = next(p for p in powned if p['name'] == 'testproj1')
    collabs = tuple(c['id'] for c in s.project_collaborator_list(prec))
    if collabs:
        s.project_collaborator_remove(prec, collabs) 
        plist = s.project_list(collaborators=True)
    assert set(len(p['collaborators'].split(', ')) if p['collaborators'] else 0
               for p in powned).issuperset((0, 1, 2))
    yield s, plist
    del s


@pytest.fixture(scope='session')
def admin_session():
    hostname, username, password = _get_vars('AE5_HOSTNAME', 'AE5_ADMIN_USERNAME', 'AE5_ADMIN_PASSWORD')
    s = AEAdminSession(hostname, username, password)
    yield s
    del s


@pytest.fixture(scope='session')
def user_session(user_setup):
    return user_setup[0]


@pytest.fixture(scope='session')
def project_list(user_setup):
    return user_setup[1]


@pytest.fixture(scope='session')
def project_list_cli(user_session):
    return _cmd('project list --collaborators')


@pytest.fixture(scope='session')
def project_dup_names(project_list_cli):
    counts = {}
    for p in project_list_cli:
        counts[p['name']] = counts.get(p['name'], 0) + 1
    return sorted(p for p, v in counts.items() if v > 1)
