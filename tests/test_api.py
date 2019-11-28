import tempfile
import requests
import time
import pytest
import os
import tarfile
import glob
import uuid

from datetime import datetime

from ae5_tools.api import AEUserSession, AEUnexpectedResponseError, AEException


class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self


def test_unexpected_response(user_session):
    with pytest.raises(AEUnexpectedResponseError) as excinfo:
        raise AEUnexpectedResponseError('string', 'https://test.me', 'string')
    exc = str(excinfo.value).strip()
    assert 'Unexpected response: string' == exc
    print(excinfo.value)
    with pytest.raises(AEUnexpectedResponseError) as excinfo:
        raise AEUnexpectedResponseError(AttrDict({
                'status_code': 404,
                'reason': 'reason',
                'headers': 'headers',
                'text': 'text'
            }), 'get', 'https://test.me',
            params='params', data='data', json='json')
    exc = [x.strip() for x in str(excinfo.value).splitlines()]
    assert 'Unexpected response: 404 reason' in exc
    assert 'headers: headers' in exc
    assert 'text: text' in exc
    assert 'params: params' in exc
    assert 'data: data' in exc
    assert 'json: json' in exc


def test_invalid_user_session():
    with pytest.raises(ValueError) as excinfo:
        AEUserSession('', '')
    assert 'Must supply hostname and username' in str(excinfo.value)


def test_project_list_df(user_session, project_list):
    df = user_session.project_list(collaborators=True, format='dataframe')
    assert len(df) == len(project_list)
    mismatch = False
    for row, row_df in zip(project_list, df.itertuples()):
        for k, v in row.items():
            if k.startswith('_'):
                continue
            v_df = getattr(row_df, k, None)
            if k in ('created', 'updated'):
                v = v.replace('T', ' ')
            if str(v) != str(v_df):
                print(f'{row["owner"]}/{row["name"]}, {k}: {v} != {v_df}')
                mismatch = True
    assert not mismatch


def test_project_info(user_session, project_list):
    for rec0 in project_list:
        id = rec0['id']
        pair = '{}/{}'.format(rec0['owner'], rec0['name'])
        rec1 = user_session.project_info(id, collaborators=True)
        rec2 = user_session.project_info(pair)
        rec3 = user_session.project_info(f'{pair}/{id}')
        assert all(rec0[k] == v for k, v in rec2.items())
        assert all(rec1[k] == v for k, v in rec2.items())
        assert rec2 == rec3


def test_project_info_errors(user_session):
    with pytest.raises(AEException) as excinfo:
        user_session.project_info('testproj1')
    assert 'Multiple projects' in str(excinfo.value)
    with pytest.raises(AEException) as excinfo:
        user_session.project_info('testproj4')
    assert 'No projects' in str(excinfo.value)


def test_project_collaborators(user_session, project_list):
    uname = 'tooltest2'
    rec = next(rec for rec in project_list if not rec['collaborators'])
    with pytest.raises(AEException) as excinfo:
        user_session.project_collaborator_info(rec, uname)
    assert f'Collaborator not found: {uname}' in str(excinfo.value)
    clist = user_session.project_collaborator_add(rec, uname)
    assert len(clist) == 1
    clist = user_session.project_collaborator_add(rec, 'everyone', group=True, read_only=True)
    assert len(clist) == 2
    assert all(c['id'] == uname and c['permission'] == 'rw' and c['type'] == 'user' or
               c['id'] == 'everyone' and c['permission'] == 'r' and c['type'] == 'group'
               for c in clist)
    clist = user_session.project_collaborator_add(rec, uname, read_only=True)
    assert len(clist) == 2
    assert all(c['id'] == uname and c['permission'] == 'r' and c['type'] == 'user' or
               c['id'] == 'everyone' and c['permission'] == 'r' and c['type'] == 'group'
               for c in clist)
    collabs = tuple(crec['id'] for crec in clist)
    clist = user_session.project_collaborator_remove(rec, collabs)
    assert len(clist) == 0
    with pytest.raises(AEException) as excinfo:
        user_session.project_collaborator_remove(rec, uname)
    assert f'Collaborator(s) not found: {uname}' in str(excinfo.value)


def test_resource_profiles(user_session):
    rlist = user_session.resource_profile_list()
    for rec in rlist:
        assert rec == user_session.resource_profile_info(rec['name'])


def test_editors(user_session):
    elist = user_session.editor_list()
    editors = set(rec['id'] for rec in elist)
    assert sum(rec['is_default'] for rec in elist) == 1
    assert editors.issuperset({'zeppelin', 'jupyterlab', 'notebook'})
    for rec in elist:
        assert rec == user_session.editor_info(rec['id'])


def test_samples(user_session):
    slist = user_session.sample_list()
    assert sum(rec['is_default'] for rec in slist) == 1
    assert sum(rec['is_template'] for rec in slist) > 1
    for rec in slist:
        rec2 = user_session.sample_info(rec['id'])
        rec3 = user_session.sample_info(rec['name'])
        assert rec == rec2 and rec == rec3


def test_sample_clone(user_session):
    uname = user_session.username
    cname = 'nlp_api'
    pname = 'testclone'
    rrec = user_session.sample_clone(cname, name=pname, wait=True)
    user_session.project_delete(rrec['id'])


@pytest.fixture(scope='module')
def downloaded_project(user_session):
    uname = user_session.username
    with tempfile.TemporaryDirectory() as tempd:
        fname = os.path.join(tempd, 'blob.tar.gz')
        fname2 = os.path.join(tempd, 'blob2.tar.gz')
        user_session.project_download(f'{uname}/testproj1', filename=fname)
        with tarfile.open(fname, 'r') as tf:
            tf.extractall(path=tempd)
        dnames = glob.glob(os.path.join(tempd, '*', 'anaconda-project.yml'))
        assert len(dnames) == 1
        dname = os.path.dirname(dnames[0])
        yield fname, fname2, dname
    for r in user_session.session_list():
        if r['owner'] == uname and r['name'].startswith('test_upload'):
            user_session.session_stop(r)
    for r in user_session.project_list():
        if r['owner'] == uname and r['name'].startswith('test_upload'):
            user_session.project_delete(r)
    assert not any(r['owner'] == uname and r['name'].startswith('test_upload')
                   for r in user_session.project_list())


def test_project_download(downloaded_project):
    pass


def test_project_upload(user_session, downloaded_project):
    fname, fname2, dname = downloaded_project
    user_session.project_upload(fname, 'test_upload1', '1.2.3', wait=True)
    rrec = user_session.revision_list('test_upload1')
    assert len(rrec) == 1
    assert rrec[0]['name'] == '1.2.3'
    user_session.project_download('test_upload1:1.2.3', filename=fname2)


def test_project_upload_as_directory(user_session, downloaded_project):
    fname, fname2, dname = downloaded_project
    user_session.project_upload(dname, 'test_upload2', '1.3.4', wait=True)
    rrec = user_session.revision_list('test_upload2')
    assert len(rrec) == 1
    assert rrec[0]['name'] == '1.3.4'
    user_session.project_download('test_upload2:1.3.4', filename=fname2)


@pytest.fixture(scope='module')
def api_project(user_session):
    uname = user_session.username
    pname = 'testproj3'
    prec = user_session.project_info(f'{uname}/{pname}')
    yield prec


def test_project_activity(user_session, api_project):
    prec = api_project
    activity = user_session.project_activity(prec, limit=-1)
    assert activity[-1]['status'] == 'created'
    assert activity[-1]['done']
    assert activity[-1]['owner'] == prec['owner']
    activity2 = user_session.project_activity(prec, latest=True)
    assert activity[0] == activity2


def test_project_revisions(user_session, api_project):
    prec = api_project
    revs = user_session.revision_list(prec)
    rev0 = user_session.revision_info(prec)
    assert revs[0] == rev0
    rev0 = user_session.revision_info(f'{prec["id"]}:latest')
    rev0['_project'].setdefault('project_create_status', 'done')
    assert revs[0] == rev0
    for rev in revs:
        revN = user_session.revision_info(f'{prec["id"]}:{rev["id"]}')
        revN['_project'].setdefault('project_create_status', 'done')
        assert rev == revN
    commands = user_session.revision_commands(prec)
    assert rev0['commands'] == ', '.join(c['id'] for c in commands)


@pytest.fixture(scope='module')
def api_session(user_session, api_project):
    prec = api_project
    srec = user_session.session_start(prec, wait=False)
    srec2 = user_session.session_restart(srec, wait=True)
    assert not any(r['id'] == srec['id'] for r in user_session.session_list())
    yield prec, srec2
    user_session.session_stop(srec2)
    assert not any(r['id'] == srec2['id'] for r in user_session.session_list())


def test_session(user_session, api_session):
    prec, srec = api_session
    assert srec['owner'] == prec['owner'], srec
    assert srec['name'] == prec['name'], srec
    # Ensure that the session can be retrieved by its project ID as well
    srec2 = user_session.session_info(f'{srec["owner"]}/*/{prec["id"]}')
    assert srec['id'] == srec2['id']
    endpoint = srec['id'].rsplit("-", 1)[-1]
    sdata = user_session._get('/', subdomain=endpoint, format='text')
    assert 'Jupyter Notebook requires JavaScript.' in sdata, sdata


def test_project_sessions(user_session, api_session):
    prec, srec = api_session
    slist = user_session.project_sessions(prec)
    assert len(slist) == 1 and slist[0]['id'] == srec['id']


def test_session_branches(user_session, api_session):
    prec, srec = api_session
    branches = user_session.session_branches(srec, format='json')
    bdict = {r['branch']: r['sha1'] for r in branches}
    assert set(bdict) == {'local', 'origin/local', 'master'}, branches
    assert bdict['local'] == bdict['master'], branches


def test_session_before_changes(user_session, api_session):
    prec, srec = api_session
    changes1 = user_session.session_changes(srec, format='json')
    assert changes1 == [], changes1
    changes2 = user_session.session_changes(srec, master=True, format='json')
    assert changes2 == [], changes2


@pytest.fixture(scope='module')
def api_deployment(user_session, api_project):
    prec = api_project
    dname = 'testdeploy'
    ename = 'testendpoint'
    drec = user_session.deployment_start(prec, name=dname, endpoint=ename,
                                         command='default', public=False, wait=False,
                                         _skip_endpoint_test=True)
    drec2 = user_session.deployment_restart(drec, wait=True)
    assert not any(r['id'] == drec['id'] for r in user_session.deployment_list())
    yield prec, drec2
    user_session.deployment_stop(drec2)
    assert not any(r['id'] == drec2['id'] for r in user_session.deployment_list())


def test_deploy(user_session, api_deployment):
    prec, drec = api_deployment
    assert drec['owner'] == prec['owner'], drec
    assert drec['project_name'] == prec['name'], drec
    for attempt in range(3):
        try:
            ldata = user_session._get('/', subdomain=drec['endpoint'], format='text')
            break
        except AEUnexpectedResponseError:
            time.sleep(attempt * 5)
            pass
    else:
        raise RuntimeError("Could not get the endpoint to respond")
    assert ldata.strip() == 'Hello Anaconda Enterprise!', ldata


def test_project_deployment(user_session, api_deployment):
    prec, drec = api_deployment
    dlist = user_session.project_deployments(prec)
    assert len(dlist) == 1 and dlist[0]['id'] == drec['id']


def test_deploy_patch(user_session, api_deployment):
    prec, drec = api_deployment
    drec2 = user_session.deployment_patch(drec, public=not drec['public'])
    assert drec2['public'] != drec['public']
    drec3 = user_session.deployment_patch(drec, public=not drec2['public'])
    assert drec3['public'] == drec['public']


def test_deploy_token(user_session, api_deployment):
    prec, drec = api_deployment
    token = user_session.deployment_token(drec)
    resp = requests.get(f'https://{drec["endpoint"]}.' + user_session.hostname,
                        headers={'Authorization': f'Bearer {token}'})
    assert resp.status_code == 200
    assert resp.text.strip() == 'Hello Anaconda Enterprise!', resp.text


def test_deploy_logs(user_session, api_deployment):
    prec, drec = api_deployment
    app_prefix = 'anaconda-app-' + drec['id'].rsplit("-", 1)[-1] + '-'
    logs = user_session.deployment_logs(drec, format='json')
    assert set(logs) == {'app', 'events', 'name', 'proxy'}, logs
    assert logs['name'].startswith(app_prefix), logs['name']
    assert 'The project is ready to run commands.' in logs['app'], logs['app']
    assert app_prefix in logs['events'], logs['events']
    assert 'App Proxy is fully operational!' in logs['proxy'], logs['proxy']


def test_deploy_duplicate(user_session, api_deployment):
    prec, drec = api_deployment
    dname = drec['name'] + '-dup'
    with pytest.raises(RuntimeError) as excinfo:
        user_session.deployment_start(prec, name=dname, endpoint=drec['endpoint'],
                                      command='default', public=False, wait=True)
    assert f'endpoint "{drec["endpoint"]}" is already in use' in str(excinfo.value)
    assert not any(r['name'] == dname for r in user_session.deployment_list())


def test_deploy_collaborators(user_session, api_deployment):
    uname = 'tooltest2'
    prec, drec = api_deployment
    clist = user_session.deployment_collaborator_list(drec)
    assert len(clist) == 0
    clist = user_session.deployment_collaborator_add(drec, uname)
    assert len(clist) == 1
    clist = user_session.deployment_collaborator_add(drec, 'everyone', group=True)
    assert len(clist) == 2
    assert all(c['id'] == uname and c['type'] == 'user' or
               c['id'] == 'everyone' and c['type'] == 'group'
               for c in clist)
    clist = user_session.deployment_collaborator_remove(drec, (uname, 'everyone'))
    assert len(clist) == 0
    with pytest.raises(AEException) as excinfo:
        user_session.deployment_collaborator_remove(drec, uname)
    assert f'Collaborator(s) not found: {uname}' in str(excinfo.value)


def test_deploy_broken(user_session, api_deployment):
    prec, drec = api_deployment
    dname = drec['name'] + '-broken'
    with pytest.raises(RuntimeError) as excinfo:
        user_session.deployment_start(prec, name=dname,
                                      command='broken', public=False,
                                      stop_on_error=True)
    assert 'Error completing deployment start: App failed to run' in str(excinfo.value)
    assert not any(r['name'] == dname for r in user_session.deployment_list())


def test_job_run1(user_session, api_project):
    prec = api_project
    uname = user_session.username
    user_session.job_create(prec, name='testjob1', command='run', run=True, wait=True)
    jrecs = user_session.job_list()
    assert len(jrecs) == 1, jrecs
    rrecs = user_session.run_list()
    assert len(rrecs) == 1, rrecs
    ldata1 = user_session.run_log(rrecs[0]['id'], format='text')
    assert ldata1.endswith('Hello Anaconda Enterprise!\n'), repr(ldata1)
    user_session.job_create(prec, name='testjob1', command='run', make_unique=True, run=True, wait=True)
    jrecs = user_session.job_list()
    assert len(jrecs) == 2, jrecs
    rrecs = user_session.run_list()
    assert len(rrecs) == 2, rrecs
    jrecs2 = user_session.project_jobs(prec)
    assert jrecs == jrecs2
    rrecs2 = user_session.project_runs(prec)
    assert rrecs == rrecs2
    for rrec in rrecs:
        user_session.run_delete(rrec['id'])
    for jrec in jrecs:
        user_session.job_delete(jrec['id'])
    assert not user_session.job_list()
    assert not user_session.run_list()


def test_job_run2(user_session):
    uname = user_session.username
    # Test cleanup mode and variables in jobs
    variables = {'INTEGRATION_TEST_KEY_1': 'value1', 'INTEGRATION_TEST_KEY_2': 'value2'}
    user_session.job_create(f'{uname}/testproj3', name='testjob2', command='run_with_env_vars',
                            variables=variables, run=True, wait=True, cleanup=True)
    # The job record should have already been deleted
    assert not user_session.job_list()
    rrecs = user_session.run_list()
    assert len(rrecs) == 1, rrecs
    ldata2 = user_session.run_log(rrecs[0]['id'], format='text')
    # Confirm that the environment variables were passed through
    outvars = dict(line.strip().replace(' ', '').split(':', 1)
                   for line in ldata2.splitlines()
                   if line.startswith('INTEGRATION_TEST_KEY_'))
    assert variables == outvars, outvars
    user_session.run_delete(rrecs[0]['id'])
    assert not user_session.run_list()


def test_login_time(admin_session, user_session):
    # The current session should already be authenticated
    now = datetime.utcnow()
    plist0 = user_session.project_list()
    user_list = admin_session.user_list()
    urec = next((r for r in user_list if r['username'] == user_session.username), None)
    assert urec is not None
    ltm1 = datetime.fromtimestamp(urec['lastLogin'] / 1000.0)
    assert ltm1 < now

    # Create new login session. This should change lastLogin
    password = os.environ.get('AE5_PASSWORD')
    user_sess2 = AEUserSession(user_session.hostname, user_session.username, password, persist=False)
    plist1 = user_sess2.project_list()
    urec = admin_session.user_info(urec['id'])
    ltm2 = datetime.fromtimestamp(urec['lastLogin'] / 1000.0)
    assert ltm2 > ltm1
    user_sess2.disconnect()
    assert plist1 == plist0

    # Create new impersonation session. This should not change lastLogin
    user_sess3 = AEUserSession(admin_session.hostname, user_session.username, admin_session, persist=False)
    plist2 = user_sess3.project_list()
    urec = admin_session.user_info(urec['id'])
    ltm3 = datetime.fromtimestamp(urec['lastLogin'] / 1000.0)
    assert ltm3 == ltm2
    user_sess3.disconnect()
    # Confirm the impersonation worked by checking the project lists are the same
    assert plist2 == plist0

    # Access the original login session. It should not reauthenticate
    plist3 = user_session.project_list()
    urec = admin_session.user_info(urec['id'])
    ltm4 = datetime.fromtimestamp(urec['lastLogin'] / 1000.0)
    assert ltm4 == ltm3
    assert plist3 == plist0
