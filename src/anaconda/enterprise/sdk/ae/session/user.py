import re

import urllib3

from .abstract import AbstractAESession

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AEUserSession(AbstractAESession):
    def _set_header(self):
        s = self.session
        for cookie in s.cookies:
            if cookie.name == "_xsrf":
                s.headers["x-xsrftoken"] = cookie.value
                break

    def _connected(self):
        return any(c.name == "_xsrf" for c in self.session.cookies)

    def _is_login(self, resp):
        if resp.status_code == 200:
            ctype = resp.headers["content-type"]
            if ctype.startswith("text/html"):
                return bool(re.search(r'<form id="kc-form-login"', resp.text, re.M))

    def _disconnect(self):
        # This will actually close out the session, so even if the cookie had
        # been captured for use elsewhere, it would no longer be useful.
        self._get("/logout")

    # TODO:
    #
    # def project_download(self, ident, filename=None):
    #     rrec = self._revision(ident, keep_latest=True)
    #     prec, rev = rrec["_project"], rrec["id"]
    #     need_filename = not bool(filename)
    #     if need_filename:
    #         revdash = f'-{rrec["name"]}' if rrec["name"] != "latest" else ""
    #         filename = f'{prec["name"]}{revdash}.tar.gz'
    #     response = self._get(f'projects/{prec["id"]}/revisions/{rev}/archive', format="blob")
    #     with open(filename, "wb") as fp:
    #         fp.write(response)
    #     if need_filename:
    #         return filename
    #
    # def project_upload(self, project_archive, name, tag, wait=True, format=None):
    #     if not name:
    #         if type(project_archive) == bytes:
    #             raise RuntimeError("Project name must be supplied for binary input")
    #         name = basename(abspath(project_archive))
    #         for suffix in (
    #             ".tar.gz",
    #             ".tar.bz2",
    #             ".tar.gz",
    #             ".zip",
    #             ".tgz",
    #             ".tbz",
    #             ".tbz2",
    #             ".tz2",
    #             ".txz",
    #         ):
    #             if name.endswith(suffix):
    #                 name = name[: -len(suffix)]
    #                 break
    #     try:
    #         f = None
    #         if type(project_archive) == bytes:
    #             f = io.BytesIO(project_archive)
    #         elif not os.path.exists(project_archive):
    #             raise RuntimeError(f"File/directory not found: {project_archive}")
    #         elif not isdir(project_archive):
    #             f = open(project_archive, "rb")
    #         elif not isfile(join(project_archive, "anaconda-project.yml")):
    #             raise RuntimeError(f"Project directory must include anaconda-project.yml")
    #         else:
    #             f = io.BytesIO()
    #             create_tar_archive(project_archive, "project", f)
    #             project_archive = project_archive + ".tar.gz"
    #         f.seek(0)
    #         data = {"name": name}
    #         if tag:
    #             data["tag"] = tag
    #         f = (project_archive, f)
    #         response = self._post_record(
    #             "projects/upload",
    #             record_type="project",
    #             api_kwargs={"files": {b"project_file": f}, "data": data},
    #         )
    #     finally:
    #         if f is not None:
    #             f[1].close()
    #     if response.get("error"):
    #         raise RuntimeError("Error uploading project: {}".format(response["error"]["message"]))
    #     if wait:
    #         self._wait(response)
    #     if response["action"]["error"]:
    #         raise RuntimeError("Error processing upload: {}".format(response["action"]["message"]))
    #     if wait:
    #         return self.project_info(response["id"], format=format, retry=True)
    #
    # def deployment_list(self, filter=None, collaborators=False, k8s=False, format=None):
    #     response = self._get_records("deployments", filter=filter, collaborators=collaborators, k8s=k8s)
    #     return self._format_response(response, format=format)
    #
    # def deployment_info(self, ident, collaborators=False, k8s=False, format=None, quiet=False):
    #     record = self.ident_record("deployment", ident, collaborators=collaborators, k8s=k8s, quiet=quiet)
    #     return self._format_response(record, format=format)
    #
    # def deployment_start(
    #     self,
    #     ident,
    #     name=None,
    #     endpoint=None,
    #     command=None,
    #     resource_profile=None,
    #     public=False,
    #     collaborators=None,
    #     wait=True,
    #     open=False,
    #     frame=False,
    #     stop_on_error=False,
    #     format=None,
    #     _skip_endpoint_test=False,
    # ):
    #     rrec = self._revision(ident, keep_latest=True)
    #     id, prec = rrec["project_id"], rrec["_project"]
    #     if command is None:
    #         command = rrec["commands"].split(",", 1)[0]
    #     if resource_profile is None:
    #         resource_profile = prec["resource_profile"]
    #     data = {
    #         "source": rrec["url"],
    #         "revision": rrec["name"],
    #         "resource_profile": resource_profile,
    #         "command": command,
    #         "public": bool(public),
    #         "target": "deploy",
    #     }
    #     if name:
    #         data["name"] = name
    #     if endpoint:
    #         if not re.match(r"[A-Za-z0-9-]+", endpoint):
    #             raise AEError(f"Invalid endpoint: {endpoint}")
    #         if not _skip_endpoint_test:
    #             try:
    #                 self._head(f"/_errors/404.html", subdomain=endpoint)
    #                 raise AEError('endpoint "{}" is already in use'.format(endpoint))
    #             except AEUnexpectedResponseError:
    #                 pass
    #         data["static_endpoint"] = endpoint
    #     response = self._post_record(f"projects/{id}/deployments", api_kwargs={"json": data})
    #     id = response["id"]
    #     if response.get("error"):
    #         raise AEError("Error starting deployment: {}".format(response["error"]["message"]))
    #     if collaborators:
    #         self.deployment_collaborator_list_set(id, collaborators)
    #     # The _wait method doesn't work here. The action isn't even updated, it seems
    #     if wait or stop_on_error:
    #         while response["state"] in ("initial", "starting"):
    #             time.sleep(2)
    #             response = self._get_records(f"deployments/{id}", record_type="deployment")
    #         if response["state"] != "started":
    #             if stop_on_error:
    #                 self.deployment_stop(id)
    #             raise AEError(f'Error completing deployment start: {response["status_text"]}')
    #     if open:
    #         self.deployment_open(response, frame)
    #     return self._format_response(response, format=format)
    #
    # def deployment_restart(self, ident, wait=True, open=False, frame=True, stop_on_error=False, format=None):
    #     drec = self.ident_record("deployment", ident)
    #     collab = self.deployment_collaborator_list(drec)
    #     if drec.get("url"):
    #         endpoint = drec["url"].split("/", 3)[2].split(".", 1)[0]
    #         if drec["id"].endswith(endpoint):
    #             endpoint = None
    #     else:
    #         endpoint = None
    #     self.deployment_stop(drec)
    #     return self.deployment_start(
    #         "{}:{}".format(drec["project_id"], drec["revision"]),
    #         endpoint=endpoint,
    #         command=drec["command"],
    #         resource_profile=drec["resource_profile"],
    #         public=drec["public"],
    #         collaborators=collab,
    #         wait=wait,
    #         open=open,
    #         frame=frame,
    #         stop_on_error=stop_on_error,
    #         format=format,
    #         _skip_endpoint_test=True,
    #     )
    #
    # def deployment_open(self, ident, frame=False):
    #     drec = self.ident_record("deployment", ident)
    #     scheme, _, hostname, _ = drec["project_url"].split("/", 3)
    #     if frame:
    #         url = f'{scheme}//{hostname}/deployments/detail/{drec["id"]}/view'
    #     else:
    #         url = drec["url"]
    #     webbrowser.open(url, 1, True)
    #
    # def deployment_patch(self, ident, format=None, **kwargs):
    #     drec = self.ident_record("deployment", ident)
    #     data = {k: v for k, v in kwargs.items() if v is not None}
    #     if data:
    #         id = drec["id"]
    #         self._patch(f"deployments/{id}", json=data)
    #         drec = self.ident_record("deployment", id)
    #     return self._format_response(drec, format=format)
    #
    # def deployment_stop(self, ident):
    #     id = self.ident_record("deployment", ident)["id"]
    #     self._delete(f"deployments/{id}")
    #
    # def deployment_logs(self, ident, which=None, format=None):
    #     id = self.ident_record("deployment", ident)["id"]
    #     response = self._get(f"deployments/{id}/logs")
    #     if which is not None:
    #         response = response[which]
    #     return self._format_response(response, format=format)
    #
    # def _pre_job(self, records):
    #     precs = {x["id"]: x for x in self._get_records("projects")}
    #     for rec in records:
    #         if rec.get("project_url"):
    #             pid = "a0-" + (rec.get("project_url") or "").rsplit("/", 1)[-1]
    #             prec = precs.get(pid, {})
    #             rec["project_id"] = pid
    #             rec["_project"] = prec
    #             rec
    #     return records
    #
    # def job_list(self, filter=None, format=None):
    #     response = self._get_records("jobs", filter=filter)
    #     return self._format_response(response, format=format)
    #
    # def job_info(self, ident, format=None, quiet=False):
    #     response = self.ident_record("job", ident, quiet=quiet)
    #     return self._format_response(response, format=format)
    #
    # def job_runs(self, ident, format=None):
    #     id = self.ident_record("job", ident)["id"]
    #     response = self._get_records(f"jobs/{id}/runs")
    #     return self._format_response(response, format=format)
    #
    # def job_delete(self, ident):
    #     id = self.ident_record("job", ident)["id"]
    #     self._delete(f"jobs/{id}")
    #
    # def job_pause(self, ident, format=None):
    #     id = self.ident_record("job", ident)["id"]
    #     response = self._post_record(f"jobs/{id}/pause", record_type="job")
    #     return self._format_response(response, format=format)
    #
    # def job_unpause(self, ident, format=format):
    #     id = self.ident_record("job", ident)["id"]
    #     response = self._post_record(f"jobs/{id}/unpause", record_type="job")
    #     return self._format_response(response, format=format)
    #
    # def job_create(
    #     self,
    #     ident,
    #     schedule=None,
    #     name=None,
    #     command=None,
    #     resource_profile=None,
    #     variables=None,
    #     run=None,
    #     wait=None,
    #     cleanup=False,
    #     make_unique=None,
    #     show_run=False,
    #     format=None,
    # ):
    #     if run is None:
    #         run = not schedule or cleanup
    #     if wait is None:
    #         wait = cleanup
    #     if cleanup and schedule:
    #         raise ValueError("cannot use cleanup=True with a scheduled job")
    #     if cleanup and (not run or not wait):
    #         raise ValueError("must specify run=wait=True with cleanup=True")
    #     rrec = self._revision(ident, keep_latest=True)
    #     prec, id = rrec["_project"], rrec["project_id"]
    #     if not command:
    #         command = rrec["commands"][0]["id"]
    #     if not resource_profile:
    #         resource_profile = rrec["_project"]["resource_profile"]
    #     # AE5's default name generator unfortunately uses colons
    #     # in the creation of its job names which causes confusion for
    #     # ae5-tools, which uses them to mark a revision identifier.
    #     # Furthermore, creating a job with the same name as an deleted
    #     # job that still has run listings causes an error.
    #     if not name:
    #         name = f'{command}-{prec["name"]}'
    #         if make_unique is None:
    #             make_unique = True
    #     if make_unique:
    #         jnames = {j["name"] for j in self._get(f"jobs")}
    #         jnames.update(j["name"] for j in self._get(f"runs"))
    #         if name in jnames:
    #             bname = name
    #             for counter in range(1, len(jnames) + 1):
    #                 name = f"{bname}-{counter}"
    #                 if name not in jnames:
    #                     break
    #     data = {
    #         "source": rrec["url"],
    #         "resource_profile": resource_profile,
    #         "command": command,
    #         "target": "deploy",
    #         "schedule": schedule,
    #         "autorun": run,
    #         "revision": rrec["name"],
    #         "name": name,
    #     }
    #     if variables:
    #         data["variables"] = variables
    #     response = self._post_record(f"projects/{id}/jobs", api_kwargs={"json": data})
    #     if response.get("error"):
    #         raise AEError("Error starting job: {}".format(response["error"]["message"]))
    #     if run:
    #         jid = response["id"]
    #         run = self._get_records(f"jobs/{jid}/runs")[-1]
    #         if wait:
    #             rid = run["id"]
    #             while run["state"] not in ("completed", "error", "failed"):
    #                 time.sleep(5)
    #                 run = self._get(f"runs/{rid}")
    #             if cleanup:
    #                 self._delete(f"jobs/{jid}")
    #         if show_run:
    #             response = run
    #     return self._format_response(response, format=format)
    #
    # def job_patch(
    #     self,
    #     ident,
    #     name=None,
    #     command=None,
    #     schedule=None,
    #     resource_profile=None,
    #     variables=None,
    #     format=None,
    # ):
    #     jrec = self.ident_record("job", ident)
    #     id = jrec["id"]
    #     data = {}
    #     if name and name != jrec["name"]:
    #         data["name"] = name
    #     if command and command != jrec["command"]:
    #         data["command"] = command
    #     if schedule and schedule != jrec["schedule"]:
    #         data["schedule"] = schedule
    #     if resource_profile and resource_profile != jrec["resource_profile"]:
    #         data["resource_profile"] = resource_profile
    #     if variables is not None and data["variables"] != jrec["variables"]:
    #         data["variables"] = variables
    #     if data:
    #         # TODO: This doesn't seem to exist ...
    #         self._patch_record(f"jobs/{id}", json=data)
    #         jrec = self.ident_record("job", id)
    #     return self._format_response(jrec, format=format)
