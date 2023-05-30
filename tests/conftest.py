# import pytest
#
# from ae5_tools.api import AEAdminSession, AEUserSession
#
# from .utils import _cmd, _get_vars
#
#
# # Expectations: the user AE5_USERNAME should have at least three projects:
# # - project names: testproj1, testproj2, testproj3
# # - all three editors should be represented
# # - the projects should have 2, 1, and 0 collaborators, respectively
# # Furthermore, there should be a second user satisfying the following:
# # - project names: testproj1, testproj2
# # - AE5_USERNAME is a collaborator on both
# @pytest.fixture(scope="session")
# def user_session():
#     hostname, username, password = _get_vars("AE5_HOSTNAME", "AE5_USERNAME", "AE5_PASSWORD")
#     s = AEUserSession(hostname, username, password)
#     for run in s.run_list():
#         if run["owner"] == username:
#             s.run_delete(run)
#     for job in s.job_list():
#         if job["owner"] == username:
#             s.job_delete(job)
#     for dep in s.deployment_list():
#         if dep["owner"] == username:
#             s.deployment_stop(dep)
#     for sess in s.session_list():
#         if sess["owner"] == username:
#             s.session_stop(sess)
#     plist = s.project_list()
#     for p in plist:
#         if p["name"] not in {"testproj1", "testproj2", "testproj3"} and p["owner"] == username:
#             s.project_delete(p["id"])
#     # Make sure testproj3 is using the Jupyter editor
#     prec = s.project_info(f"{username}/testproj3", collaborators=True)
#     if prec["editor"] != "notebook" or prec["resource_profile"] != "default":
#         s.project_patch(prec, editor="notebook", resource_profile="default")
#     # Make sure testproj3 has no collaborators
#     if prec["_collaborators"]:
#         collabs = tuple(c["id"] for c in prec["_collaborators"])
#         s.project_collaborator_remove(prec, collabs)
#         plist = s.project_list(collaborators=True)
#     plist = s.project_list(collaborators=True)
#     powned = [p for p in plist if p["owner"] == username]
#     pother = [p for p in plist if p["owner"] != username]
#     # Assert there are exactly 3 projects owned by the test user
#     assert len(powned) == 3
#     # Need at least two duplicated project names to properly test sorting/filtering
#     assert len(set(p["name"] for p in powned).intersection(p["name"] for p in pother)) >= 2
#     # Make sure all three editors are represented
#     assert len(set(p["editor"] for p in powned)) == 3
#     # Make sure we have 0, 1, and 2 collaborators represented
#     assert set(len(p["_collaborators"]) for p in plist if p["owner"] == username).issuperset((0, 1, 2))
#     yield s
#     s.disconnect()
#
#
# @pytest.fixture(scope="session")
# def admin_session():
#     hostname, username, password = _get_vars("AE5_HOSTNAME", "AE5_ADMIN_USERNAME", "AE5_ADMIN_PASSWORD")
#     s = AEAdminSession(hostname, username, password)
#     yield s
#     del s
