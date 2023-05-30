# import glob
# import io
# import os
# import tarfile
# import tempfile
# import time
# import uuid
# from datetime import datetime
#
# import pytest
# import requests
#
# from ae5_tools.api import AEException, AEUnexpectedResponseError, AEUserSession
#
# from .utils import _compare_tarfiles, _get_vars
#
#
# class AttrDict(dict):
#     def __init__(self, *args, **kwargs):
#         super(AttrDict, self).__init__(*args, **kwargs)
#         self.__dict__ = self
#
#
# def test_unexpected_response(user_session):
#     with pytest.raises(AEUnexpectedResponseError) as excinfo:
#         raise AEUnexpectedResponseError("string", "https://test.me", "string")
#     exc = str(excinfo.value).strip()
#     assert "Unexpected response: string" == exc
#     print(excinfo.value)
#     with pytest.raises(AEUnexpectedResponseError) as excinfo:
#         raise AEUnexpectedResponseError(
#             AttrDict({"status_code": 404, "reason": "reason", "headers": "headers", "text": "text"}),
#             "get",
#             "https://test.me",
#             params="params",
#             data="data",
#             json="json",
#         )
#     exc = [x.strip() for x in str(excinfo.value).splitlines()]
#     assert "Unexpected response: 404 reason" in exc
#     assert "headers: headers" in exc
#     assert "text: text" in exc
#     assert "params: params" in exc
#     assert "data: data" in exc
#     assert "json: json" in exc
#
#
# def test_user_session(monkeypatch, capsys):
#     with pytest.raises(ValueError) as excinfo:
#         AEUserSession("", "")
#     assert "Must supply hostname and username" in str(excinfo.value)
#     hostname, username, password = _get_vars("AE5_HOSTNAME", "AE5_USERNAME", "AE5_PASSWORD")
#     with pytest.raises(AEException) as excinfo:
#         c = AEUserSession(hostname, username, "x" + password, persist=False)
#         c.authorize()
#         del c
#     assert "Invalid username or password." in str(excinfo.value)
#     passwords = [password, "", "x" + password]
#     monkeypatch.setattr("getpass.getpass", lambda x: passwords.pop())
#     c = AEUserSession(hostname, username, persist=False)
#     c.authorize()
#     captured = capsys.readouterr()
#     assert f"Password for {username}@{hostname}" in captured.err
#     assert f"Invalid username or password; please try again." in captured.err
#     assert f"Must supply a password" in captured.err
#     true_endpoint, c._k8s_endpoint = c._k8s_endpoint, "ssh:fakeuser"
#     with pytest.raises(AEException) as excinfo:
#         c._k8s("status")
#     assert "Error establishing k8s connection" in str(excinfo.value)
#     c._k8s_endpoint = "fakek8sendpoint"
#     with pytest.raises(AEException) as excinfo:
#         c._k8s("status")
#     assert "No deployment found at endpoint fakek8sendpoint" in str(excinfo.value)
#     with pytest.raises(AEException) as excinfo:
#         c._k8s("status")
#     assert "No k8s connection available" in str(excinfo.value)
#     c._k8s_endpoint = true_endpoint
#     assert c._k8s("status") == "Alive and kicking"
#
#
# @pytest.fixture(scope="module")
# def project_list(user_session):
#     return user_session.project_list(collaborators=True)
#
#
# def test_project_list_df(user_session, project_list):
#     with pytest.raises(ImportError) as excinfo:
#         df = user_session.project_list(collaborators=True, format="_dataframe")
#     assert 'Pandas must be installed in order to use format="dataframe"' in str(excinfo.value)
#     df = user_session.project_list(collaborators=True, format="dataframe")
#     assert len(df) == len(project_list)
#     mismatch = False
#     for row, row_df in zip(project_list, df.itertuples()):
#         for k, v in row.items():
#             if k.startswith("_"):
#                 continue
#             v_df = getattr(row_df, k, None)
#             if k in ("created", "updated"):
#                 v = v.replace("T", " ")
#             if str(v) != str(v_df):
#                 print(f'{row["owner"]}/{row["name"]}, {k}: {v} != {v_df}')
#                 mismatch = True
#     assert not mismatch
#
#
# def test_project_info(user_session, project_list):
#     for rec0 in project_list:
#         id = rec0["id"]
#         pair = "{}/{}".format(rec0["owner"], rec0["name"])
#         rec1 = user_session.project_info(id, collaborators=True)
#         rec2 = user_session.project_info(pair)
#         rec3 = user_session.project_info(f"{pair}/{id}")
#         assert all(rec0[k] == v for k, v in rec2.items())
#         assert all(rec1[k] == v for k, v in rec2.items())
#         assert rec2 == rec3
#
#
# def test_project_info_errors(user_session, project_list):
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_info("testproj1")
#     assert "Multiple projects" in str(excinfo.value)
#     user_session.project_info("testproj1", quiet=True)
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_info("testproj4")
#     assert "No projects" in str(excinfo.value)
#     user_session.project_info("testproj4", quiet=True)
#
#
# @pytest.fixture(scope="module")
# def resource_profiles(user_session):
#     return user_session.resource_profile_list()
#
#
# def test_resource_profiles(user_session, resource_profiles):
#     for rec in resource_profiles:
#         assert rec == user_session.resource_profile_info(rec["name"])
#     with pytest.raises(AEException) as excinfo:
#         user_session.resource_profile_info("*")
#     assert "Multiple resource profiles found" in str(excinfo.value)
#     with pytest.raises(AEException) as excinfo:
#         user_session.resource_profile_info("")
#     assert "No resource profiles found" in str(excinfo.value)
#
#
# @pytest.fixture(scope="module")
# def editors(user_session):
#     return user_session.editor_list()
#
#
# def test_editors(user_session, editors):
#     for rec in editors:
#         assert rec == user_session.editor_info(rec["id"])
#     assert sum(rec["is_default"] for rec in editors) == 1
#     assert set(rec["id"] for rec in editors).issuperset({"jupyterlab", "notebook"})
#
#
# @pytest.mark.xfail
# def test_endpoints(user_session):
#     slist = user_session.endpoint_list()
#     for rec in slist:
#         rec2 = user_session.endpoint_info(rec["id"])
#         assert rec == rec2
#
#
# def test_samples(user_session):
#     slist = user_session.sample_list()
#     assert sum(rec["is_default"] for rec in slist) == 1
#     assert sum(rec["is_template"] for rec in slist) > 1
#     for rec in slist:
#         rec2 = user_session.sample_info(rec["id"])
#         rec3 = user_session.sample_info(rec["name"])
#         assert rec == rec2 and rec == rec3
#
#
# def test_sample_clone(user_session):
#     cname = "NLP API"
#     pname = "testclone"
#     rrec1 = user_session.sample_clone(cname, name=pname, wait=True)
#     with pytest.raises(AEException) as excinfo:
#         user_session.sample_clone(cname, name=pname, wait=True)
#     rrec2 = user_session.sample_clone(cname, name=pname, make_unique=True, wait=True)
#     rrec3 = user_session.sample_clone(cname, wait=True)
#     user_session.project_delete(rrec1)
#     user_session.project_delete(rrec2)
#     user_session.project_delete(rrec3)
#
#
# @pytest.fixture(scope="module")
# def api_project(user_session, project_list):
#     return next(rec for rec in project_list if rec["name"] == "testproj3")
#
#
# @pytest.fixture(scope="module")
# def api_revisions(user_session, api_project):
#     prec = api_project
#     revs = user_session.revision_list(prec)
#     return prec, revs
#
#
# @pytest.fixture(scope="module")
# def downloaded_project(user_session, api_revisions):
#     prec, revs = api_revisions
#     with tempfile.TemporaryDirectory() as tempd:
#         fname = user_session.project_download(prec)
#         assert fname == prec["name"] + ".tar.gz"
#         with tarfile.open(fname, "r") as tf:
#             tf.extractall(path=tempd)
#         dnames = glob.glob(os.path.join(tempd, "*", "anaconda-project.yml"))
#         assert len(dnames) == 1
#         dname = os.path.dirname(dnames[0])
#         yield fname, dname
#     for r in user_session.session_list():
#         if r["name"].startswith("test_upload"):
#             user_session.session_stop(r)
#     for r in user_session.project_list():
#         if r["name"].startswith("test_upload"):
#             user_session.project_delete(r)
#     assert not any(r["name"].startswith("test_upload") for r in user_session.project_list())
#
#
# def test_project_download(user_session, downloaded_project):
#     # Use this to exercise a couple of branches in _api
#     pass
#
#
# def test_project_upload(user_session, downloaded_project):
#     fname, dname = downloaded_project
#     user_session.project_upload(fname, "test_upload1", "1.2.3", wait=True)
#     rrec = user_session.revision_list("test_upload1")
#     rev = rrec[0]["name"]
#     fname2 = user_session.project_download(f"test_upload1:{rev}")
#     assert fname2 == f"test_upload1-{rev}.tar.gz"
#     assert os.path.exists(fname2)
#     _compare_tarfiles(fname, fname2)
#     if rev == "0.0.1":
#         pytest.xfail("5.4.1 revision issue")
#     assert rev == "1.2.3"
#
#
# def test_project_upload_as_directory(user_session, downloaded_project):
#     fname, dname = downloaded_project
#     user_session.project_upload(dname, "test_upload2", "1.3.4", wait=True)
#     rrec = user_session.revision_list("test_upload2")
#     assert len(rrec) == 1
#     rev = rrec[0]["name"]
#     fname2 = user_session.project_download(f"test_upload2:{rev}")
#     assert fname2 == f"test_upload2-{rev}.tar.gz"
#     assert os.path.exists(fname2)
#     if rev == "0.0.1":
#         pytest.xfail("5.4.1 revision issue")
#     assert rev == "1.3.4"
#
#
# def _soft_equal(d1, d2):
#     if isinstance(d1, dict) and isinstance(d2, dict):
#         for k in set(d1) | set(d2):
#             if k in d1 and k in d2:
#                 if not _soft_equal(d1[k], d2[k]):
#                     return False
#         return True
#     else:
#         return d1 == d2
#
#
# def test_project_revisions(user_session, api_revisions):
#     prec, revs = api_revisions
#     rev0 = user_session.revision_info(prec)
#     # There are sometimes minor differences in the '_project'
#     # record due to the exact way it is retrieved. For instance,
#     # the project_create_status value will be missing in the
#     # info calls; and prec has the collaborators entries it.
#     # So we do a rougher verification that the project entry
#     # is correct.
#     assert _soft_equal(rev0["_project"], revs[0]["_project"])
#     rev0["_project"] = revs[0]["_project"]
#     assert rev0 == revs[0]
#     rev0 = user_session.revision_info(f'{prec["id"]}:latest')
#     assert _soft_equal(rev0["_project"], revs[0]["_project"])
#     rev0["_project"] = revs[0]["_project"]
#     assert revs[0] == rev0
#     for rev in revs:
#         revN = user_session.revision_info(f'{prec["id"]}:{rev["id"]}')
#         assert _soft_equal(revN["_project"], rev["_project"])
#         revN["_project"] = rev["_project"]
#         assert rev == revN
#     commands = user_session.revision_commands(prec)
#     assert rev0["commands"] == ", ".join(c["id"] for c in commands)
#
#
# def test_project_revision_errors(user_session, api_revisions):
#     prec, revs = api_revisions
#     with pytest.raises(AEException) as excinfo:
#         user_session.revision_info("testproj1")
#     user_session.revision_info("testproj1", quiet=True)
#     assert "Multiple projects" in str(excinfo.value)
#     with pytest.raises(AEException) as excinfo:
#         user_session.revision_info("testproj4")
#     assert "No projects" in str(excinfo.value)
#     user_session.revision_info("testproj4", quiet=True)
#     with pytest.raises(AEException) as excinfo:
#         user_session.revision_info(f'{prec["id"]}:0.*')
#     assert "Multiple revisions" in str(excinfo.value)
#     user_session.revision_info(f'{prec["id"]}:0.*', quiet=True)
#     with pytest.raises(AEException) as excinfo:
#         user_session.revision_info(f'{prec["id"]}:a.b.c')
#     assert "No revisions" in str(excinfo.value)
#     user_session.revision_info(f'{prec["id"]}:a.b.c', quiet=True)
#
#
# def test_project_patch(user_session, api_project, editors, resource_profiles):
#     prec = api_project
#     old, new = {}, {}
#     for what, wlist in (
#         ("resource_profile", (r["name"] for r in resource_profiles)),
#         ("editor", (e["id"] for e in editors)),
#     ):
#         old[what] = prec[what]
#         new[what] = next(v for v in wlist if v != old)
#     prec2 = user_session.project_patch(prec, **new)
#     assert {k: prec2[k] for k in new} == new
#     prec3 = user_session.project_patch(prec2, **old)
#     assert {k: prec3[k] for k in old} == old
#
#
# def test_project_collaborators(user_session, api_project, project_list):
#     prec = api_project
#     uname = next(rec["owner"] for rec in project_list if rec["owner"] != prec["owner"])
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_collaborator_info(prec, uname)
#     user_session.project_collaborator_info(prec, uname, quiet=True)
#     assert f"No collaborators found matching id={uname}" in str(excinfo.value)
#     clist = user_session.project_collaborator_add(prec, uname)
#     assert len(clist) == 1
#     clist = user_session.project_collaborator_add(prec, "everyone", group=True, read_only=True)
#     assert len(clist) == 2
#     assert all(
#         c["id"] == uname
#         and c["permission"] == "rw"
#         and c["type"] == "user"
#         or c["id"] == "everyone"
#         and c["permission"] == "r"
#         and c["type"] == "group"
#         for c in clist
#     )
#     clist = user_session.project_collaborator_add(prec, uname, read_only=True)
#     assert len(clist) == 2
#     assert all(
#         c["id"] == uname
#         and c["permission"] == "r"
#         and c["type"] == "user"
#         or c["id"] == "everyone"
#         and c["permission"] == "r"
#         and c["type"] == "group"
#         for c in clist
#     )
#     collabs = tuple(crec["id"] for crec in clist)
#     clist = user_session.project_collaborator_remove(prec, collabs)
#     assert len(clist) == 0
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_collaborator_remove(prec, uname)
#     assert f"Collaborator(s) not found: {uname}" in str(excinfo.value)
#
#
# def test_project_activity(user_session, api_project):
#     prec = api_project
#     activity = user_session.project_activity(prec)
#     assert 1 <= len(activity) <= 10
#     activity2 = user_session.project_activity(prec, latest=True)
#     assert activity[0] == activity2
#     activity3 = user_session.project_activity(prec, limit=1)
#     assert activity[0] == activity3[0]
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_activity(prec, latest=True, all=True)
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_activity(prec, latest=True, limit=2)
#     with pytest.raises(AEException) as excinfo:
#         user_session.project_activity(prec, all=True, limit=2)
#
#
# @pytest.fixture(scope="module")
# def api_session(user_session, api_project):
#     prec = api_project
#     srec = user_session.session_start(prec, wait=False)
#     srec2 = user_session.session_restart(srec, wait=True)
#     assert not any(r["id"] == srec["id"] for r in user_session.session_list())
#     yield prec, srec2
#     user_session.session_stop(srec2)
#     assert not any(r["id"] == srec2["id"] for r in user_session.session_list())
#
#
# def test_session(user_session, api_session):
#     prec, srec = api_session
#     assert srec["owner"] == prec["owner"], srec
#     assert srec["name"] == prec["name"], srec
#     # Ensure that the session can be retrieved by its project ID as well
#     srec2 = user_session.session_info(f'{srec["owner"]}/*/{prec["id"]}')
#     assert srec["id"] == srec2["id"]
#     endpoint = srec["id"].rsplit("-", 1)[-1]
#     sdata = user_session._get("/", subdomain=endpoint, format="text")
#     assert "Jupyter Notebook requires JavaScript." in sdata, sdata
#
#
# def test_project_sessions(user_session, api_session):
#     prec, srec = api_session
#     slist = user_session.project_sessions(prec)
#     assert len(slist) == 1 and slist[0]["id"] == srec["id"]
#
#
# def test_session_branches(user_session, api_session):
#     prec, srec = api_session
#     branches = user_session.session_branches(srec, format="json")
#     bdict = {r["branch"]: r["sha1"] for r in branches}
#     assert set(bdict) == {"local", "origin/local", "master"}, branches
#     assert bdict["local"] == bdict["master"], branches
#
#
# def test_session_before_changes(user_session, api_session):
#     prec, srec = api_session
#     changes1 = user_session.session_changes(srec, format="json")
#     changes1 = [c for c in changes1 if c["path"] != ".projectignore"]
#     assert changes1 == [], changes1
#     changes2 = user_session.session_changes(srec, master=True, format="json")
#     changes2 = [c for c in changes2 if c["path"] != ".projectignore"]
#     assert changes2 == [], changes2
#
#
# @pytest.fixture(scope="module")
# def api_deployment(user_session, api_project):
#     prec = api_project
#     dname = "testdeploy"
#     ename = "testendpoint"
#     drec = user_session.deployment_start(
#         prec, name=dname, endpoint=ename, command="default", public=False, wait=False, _skip_endpoint_test=True
#     )
#     drec2 = user_session.deployment_restart(drec, wait=True)
#     assert not any(r["id"] == drec["id"] for r in user_session.deployment_list())
#     yield prec, drec2
#     user_session.deployment_stop(drec2)
#     assert not any(r["id"] == drec2["id"] for r in user_session.deployment_list())
#
#
# def test_deploy(user_session, api_deployment):
#     prec, drec = api_deployment
#     assert drec["owner"] == prec["owner"], drec
#     assert drec["project_name"] == prec["name"], drec
#     for attempt in range(3):
#         try:
#             ldata = user_session._get("/", subdomain=drec["endpoint"], format="text")
#             break
#         except AEUnexpectedResponseError:
#             time.sleep(attempt * 5)
#             pass
#     else:
#         raise RuntimeError("Could not get the endpoint to respond")
#     assert ldata.strip() == "Hello Anaconda Enterprise!", ldata
#
#
# def test_project_deployment(user_session, api_deployment):
#     prec, drec = api_deployment
#     dlist = user_session.project_deployments(prec)
#     assert len(dlist) == 1 and dlist[0]["id"] == drec["id"]
#
#
# def test_deploy_patch(user_session, api_deployment):
#     prec, drec = api_deployment
#     drec2 = user_session.deployment_patch(drec, public=not drec["public"])
#     assert drec2["public"] != drec["public"]
#     drec3 = user_session.deployment_patch(drec, public=not drec2["public"])
#     assert drec3["public"] == drec["public"]
#
#
# def test_deploy_token(user_session, api_deployment):
#     prec, drec = api_deployment
#     token = user_session.deployment_token(drec)
#     resp = requests.get(
#         f'https://{drec["endpoint"]}.' + user_session.hostname,
#         headers={"Authorization": f"Bearer {token}"},
#         verify=False,
#     )
#     assert resp.status_code == 200
#     assert resp.text.strip() == "Hello Anaconda Enterprise!", resp.text
#     with pytest.raises(AEException) as excinfo:
#         token = user_session.deployment_token(drec, format="table")
#     assert "Response is not a tabular format" in str(excinfo.value)
#
#
# def test_deploy_logs(user_session, api_deployment):
#     prec, drec = api_deployment
#     app_prefix = "anaconda-app-" + drec["id"].rsplit("-", 1)[-1] + "-"
#     logs = user_session.deployment_logs(drec, format="json")
#     assert set(logs) == {"app", "events", "name", "proxy"}, logs
#     assert logs["name"].startswith(app_prefix), logs["name"]
#     assert "The project is ready to run commands." in logs["app"], logs["app"]
#     assert app_prefix in logs["events"], logs["events"]
#     assert "App Proxy is fully operational!" in logs["proxy"], logs["proxy"]
#
#
# def test_deploy_duplicate(user_session, api_deployment):
#     prec, drec = api_deployment
#     dname = drec["name"] + "-dup"
#     with pytest.raises(RuntimeError) as excinfo:
#         user_session.deployment_start(
#             prec, name=dname, endpoint=drec["endpoint"], command="default", public=False, wait=True
#         )
#     assert f'endpoint "{drec["endpoint"]}" is already in use' in str(excinfo.value)
#     assert not any(r["name"] == dname for r in user_session.deployment_list())
#
#
# def test_deploy_collaborators(user_session, api_deployment):
#     uname = "tooltest2"
#     prec, drec = api_deployment
#     clist = user_session.deployment_collaborator_list(drec)
#     assert len(clist) == 0
#     clist = user_session.deployment_collaborator_add(drec, uname)
#     assert len(clist) == 1
#     clist = user_session.deployment_collaborator_add(drec, "everyone", group=True)
#     assert len(clist) == 2
#     clist = user_session.deployment_collaborator_add(drec, uname)
#     assert len(clist) == 2
#     assert all(
#         c["id"] == uname and c["type"] == "user" or c["id"] == "everyone" and c["type"] == "group" for c in clist
#     )
#     for crec in clist:
#         crec2 = user_session.deployment_collaborator_info(drec, crec["id"])
#         assert crec2["id"] == crec["id"] and crec2["type"] == crec["type"]
#     clist = user_session.deployment_collaborator_remove(drec, (uname, "everyone"))
#     assert len(clist) == 0
#     with pytest.raises(AEException) as excinfo:
#         user_session.deployment_collaborator_remove(drec, uname)
#     assert f"Collaborator(s) not found: {uname}" in str(excinfo.value)
#
#
# def test_deploy_broken(user_session, api_deployment):
#     prec, drec = api_deployment
#     dname = drec["name"] + "-broken"
#     with pytest.raises(RuntimeError) as excinfo:
#         user_session.deployment_start(prec, name=dname, command="broken", public=False, stop_on_error=True)
#     assert "Error completing deployment start: App failed to run" in str(excinfo.value)
#     assert not any(r["name"] == dname for r in user_session.deployment_list())
#
#
# def test_k8s_node(user_session):
#     nlist = user_session.node_list()
#     for nrec in nlist:
#         nrec2 = user_session.node_info(nrec["name"])
#         assert nrec2["name"] == nrec["name"]
#
#
# def test_k8s_pod(user_session, api_session, api_deployment):
#     _, srec = api_session
#     _, drec = api_deployment
#     plist = user_session.pod_list()
#     assert any(prec["id"] == srec["id"] for prec in plist)
#     assert any(prec["id"] == drec["id"] for prec in plist)
#     for prec in plist:
#         prec2 = user_session.pod_info(prec["id"])
#         assert prec2["id"] == prec["id"]
#     srec2 = user_session.session_info(srec["id"], k8s=True)
#     assert srec2["id"] == srec["id"]
#     drec2 = user_session.deployment_info(drec["id"], k8s=True)
#     assert drec2["id"] == drec["id"]
#
#
# def test_job_run1(user_session, api_project):
#     prec = api_project
#     uname = user_session.username
#     user_session.job_create(prec, name="testjob1", command="run", run=True, wait=True)
#     jrecs = user_session.job_list()
#     assert len(jrecs) == 1, jrecs
#     rrecs = user_session.run_list()
#     assert len(rrecs) == 1, rrecs
#     ldata1 = user_session.run_log(rrecs[0]["id"], format="text")
#     assert ldata1.endswith("Hello Anaconda Enterprise!\n"), repr(ldata1)
#     user_session.job_create(prec, name="testjob1", command="run", make_unique=True, run=True, wait=True)
#     jrecs = user_session.job_list()
#     assert len(jrecs) == 2, jrecs
#     jrecs2 = user_session.project_jobs(prec)
#     assert {r["id"]: r for r in jrecs} == {r["id"]: r for r in jrecs2}
#     rrecs = user_session.run_list()
#     rrecs2 = user_session.project_runs(prec)
#     assert len(rrecs) == 2, rrecs
#     assert {r["id"]: r for r in rrecs} == {r["id"]: r for r in rrecs2}
#     for rrec in rrecs:
#         user_session.run_delete(rrec["id"])
#     for jrec in jrecs:
#         user_session.job_delete(jrec["id"])
#     assert not user_session.job_list()
#     assert not user_session.run_list()
#
#
# def test_job_run2(user_session, api_project):
#     prec = api_project
#     # Test cleanup mode and variables in jobs
#     variables = {"INTEGRATION_TEST_KEY_1": "value1", "INTEGRATION_TEST_KEY_2": "value2"}
#     user_session.job_create(
#         prec, name="testjob2", command="run_with_env_vars", variables=variables, run=True, wait=True, cleanup=True
#     )
#     # The job, and run records should have already been deleted
#     assert not user_session.job_list()
#     assert not user_session.run_list()
#
#
# def test_job_run3(user_session, api_project):
#     prec = api_project
#     # Test cleanup mode and variables in jobs
#     variables = {"INTEGRATION_TEST_KEY_1": "value1", "INTEGRATION_TEST_KEY_2": "value2"}
#     job_create_response = user_session.job_create(
#         prec, name="testjob2", command="run_with_env_vars", variables=variables, run=True, wait=True, cleanup=False
#     )
#
#     rrecs = user_session.run_list()
#     assert len(rrecs) == 1, rrecs
#     ldata2 = user_session.run_log(rrecs[0]["id"], format="text")
#     # Confirm that the environment variables were passed through
#     outvars = dict(
#         line.strip().replace(" ", "").split(":", 1)
#         for line in ldata2.splitlines()
#         if line.startswith("INTEGRATION_TEST_KEY_")
#     )
#     assert variables == outvars, outvars
#     user_session.run_delete(rrecs[0]["id"])
#     assert not user_session.run_list()
#
#     user_session.job_delete(job_create_response["id"])
#     assert not user_session.job_list()
#
#
# def test_login_time(admin_session, user_session):
#     # The current session should already be authenticated
#     now = datetime.now()
#     plist0 = user_session.project_list()
#     user_list = admin_session.user_list()
#     urec = next((r for r in user_list if r["username"] == user_session.username), None)
#     assert urec is not None
#     ltm1 = datetime.fromtimestamp(urec["lastLogin"] / 1000.0)
#     assert ltm1 < now
#
#     # Create new login session. This should change lastLogin
#     password = os.environ.get("AE5_PASSWORD")
#     user_sess2 = AEUserSession(user_session.hostname, user_session.username, password, persist=False)
#     plist1 = user_sess2.project_list()
#     urec = admin_session.user_info(urec["id"])
#     ltm2 = datetime.fromtimestamp(urec["lastLogin"] / 1000.0)
#     assert ltm2 > ltm1
#     user_sess2.disconnect()
#     assert plist1 == plist0
#
#     # Create new impersonation session. This should not change lastLogin
#     user_sess3 = AEUserSession(admin_session.hostname, user_session.username, admin_session, persist=False)
#     plist2 = user_sess3.project_list()
#     urec = admin_session.user_info(urec["id"])
#     ltm3 = datetime.fromtimestamp(urec["lastLogin"] / 1000.0)
#     assert ltm3 == ltm2
#     user_sess3.disconnect()
#     # Confirm the impersonation worked by checking the project lists are the same
#     assert plist2 == plist0
#
#     # Access the original login session. It should not reauthenticate
#     plist3 = user_session.project_list()
#     urec = admin_session.user_info(urec["id"])
#     ltm4 = datetime.fromtimestamp(urec["lastLogin"] / 1000.0)
#     assert ltm4 == ltm3
#     assert plist3 == plist0
