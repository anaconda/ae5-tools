import io
import os
import re
import time
import webbrowser
from os.path import abspath, basename, isdir, isfile, join
from tempfile import TemporaryDirectory
from typing import Any, Optional, Union

import urllib3

from ...cluster.identifier import Identifier
from ...common.config.environment import demand_env_var_as_int, get_env_var
from ...contracts.dto.error.ae_error import AEError
from ...contracts.dto.error.ae_unexpected_response_error import AEUnexpectedResponseError
from ...contracts.dto.requests.deployment_token import DeploymentTokenRequest
from ...contracts.dto.responses.deployment_token import DeploymentTokenResponse
from ...k8s.client.ae_k8s_local_client import AEK8SLocalClient
from ...k8s.client.ae_k8s_remote_client import AEK8SRemoteClient
from .abstract import AbstractAESession
from .utils.archiver import create_tar_archive
from .utils.docker import build_image, get_condarc, get_dockerfile
from .utils.empty_record_list import EmptyRecordList

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class AEUserSession(AbstractAESession):
    k8s_endpoint: str = "k8s"
    k8s_client: Optional[Union[AEK8SLocalClient, AEK8SRemoteClient]] = None

    def __init__(
        self,
        k8s_endpoint: str = "k8s",
        k8s_client: Optional[Union[AEK8SLocalClient, AEK8SRemoteClient]] = None,
        **data: Any,
    ):
        super().__init__(**data)

        env_var: Optional[str] = get_env_var(name="AE5_K8S_ENDPOINT")
        if env_var:
            self.k8s_endpoint = env_var

    def _k8s(self, method, *args, **kwargs):
        quiet = kwargs.pop("quiet", False)
        if self.k8s_client is None and self.k8s_endpoint is not None:
            if self.k8s_endpoint.startswith("ssh:"):
                username = self.k8s_endpoint[4:]
                self.k8s_client = AEK8SLocalClient(self.hostname, username)
            else:
                self.k8s_client = AEK8SRemoteClient(self, self.k8s_endpoint)
            estr = self.k8s_client.error()
            if estr:
                del self.k8s_client
                self._k8s_endpoint = self.k8s_client = None
                msg = ["Error establishing k8s connection:"]
                msg.extend("  " + x for x in estr.splitlines())
                raise AEError("\n".join(msg))
        if self.k8s_client is None:
            raise AEError("No k8s connection available")
        return getattr(self.k8s_client, method)(*args, **kwargs)

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
        if self.k8s_client is not None:
            self.k8s_client.disconnect()
            del self.k8s_client
            self.k8s_client = None

    def _api_records(self, method, endpoint, filter=None, **kwargs):
        record_type = kwargs.pop("record_type", None)
        api_kwargs = kwargs.pop("api_kwargs", None) or {}
        retry_if_empty = kwargs.pop("retry_if_empty", False)
        if not record_type:
            record_type = endpoint.rsplit("/", 1)[-1].rstrip("s")
        for attempt in range(20):
            records = self._api(method, endpoint, **api_kwargs)
            if records or not retry_if_empty:
                break
            time.sleep(0.25)
        else:
            raise AEError(f"Unexpected empty {record_type} recordset")
        return self._fix_records(record_type, records, filter, **kwargs)

    def _get_records(self, endpoint, filter=None, **kwargs):
        return self._api_records("get", endpoint, filter=filter, **kwargs)

    def _post_record(self, endpoint, filter=None, **kwargs):
        return self._api_records("post", endpoint, filter=filter, **kwargs)

    def _post_project(self, records, collaborators=False):
        if collaborators:
            self._join_collaborators("projects", records)
        return records

    def secret_add(self, key, value):
        self._post("credentials/user", json={"key": key, "value": value})

    def secret_delete(self, key):
        secrets = self.secret_list()
        if key not in secrets:
            raise AEError(f"User secret {key!r} was not found and cannot be deleted.")
        self._delete(f"credentials/user/{key}")

    def secret_list(self):
        records = self._get("credentials/user")
        if "data" in records:
            return records["data"]
        else:
            raise AEError("Secrets endpoint did not return data.")

    def project_list(self, filter=None, collaborators=False, format=None):
        records = self._get_records("projects", filter, collaborators=collaborators)
        return self._format_response(records, format=format)

    def project_info(self, ident, collaborators=False, format=None, quiet=False, retry=False):
        # Retry loop added because project creation is now so fast that the API
        # often needs time to catch up before it "sees" the new project. We only
        # use the retry loop in project creation commands for that reason.
        while True:
            try:
                record = self.ident_record("project", ident, collaborators=collaborators, quiet=quiet)
                break
            except AEError as exc:
                if not retry or not str(exc).startswith("No projects found matching id"):
                    raise
                time.sleep(0.25)
        return self._format_response(record, format=format)

    def project_patch(self, ident, format=None, **kwargs):
        prec = self.ident_record("project", ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            id = prec["id"]
            self._patch(f"projects/{id}", json=data)
            prec = self.ident_record("project", id)
        return self._format_response(prec, format=format)

    def project_delete(self, ident):
        id = self.ident_record("project", ident)["id"]
        self._delete(f"projects/{id}")

    def project_collaborator_list(self, ident, filter=None, format=None):
        id = self.ident_record("project", ident)["id"]
        response = self._get_records(f"projects/{id}/collaborators", filter)
        return self._format_response(response, format=format)

    def project_collaborator_info(self, ident, userid, quiet=False, format=None):
        filter = f"id={userid}"
        response = self.project_collaborator_list(ident, filter=filter)
        response = self._should_be_one(response, filter, quiet)
        return self._format_response(response, format=format)

    def project_collaborator_list_set(self, ident, collabs, format=None):
        id = self.ident_record("project", ident)["id"]
        result = self._put(f"projects/{id}/collaborators", json=collabs)
        if result["action"]["error"] or "collaborators" not in result:
            raise AEError(f"Unexpected error adding collaborator: {result}")
        result = self._fix_records("collaborator", result["collaborators"])
        return self._format_response(result, format=format)

    def project_collaborator_add(self, ident, userid, group=False, read_only=False, format=None):
        prec = self.ident_record("project", ident)
        collabs = self.project_collaborator_list(prec)
        cmap = {c["id"]: (c["type"], c["permission"]) for c in collabs}
        if not isinstance(userid, tuple):
            userid = (userid,)
        tp = ("group" if group else "user", "r" if read_only else "rw")
        nmap = {k: tp for k in userid if k not in cmap}
        nmap.update((k, v) for k, v in cmap.items() if k not in userid or v == tp)
        if nmap != cmap:
            collabs = [{"id": k, "type": t, "permission": p} for k, (t, p) in nmap.items()]
            collabs = self.project_collaborator_list_set(prec, collabs)
        if any(k not in nmap for k in userid):
            nmap.update((k, tp) for k in userid)
            collabs = [{"id": k, "type": t, "permission": p} for k, (t, p) in nmap.items()]
            collabs = self.project_collaborator_list_set(prec, collabs)
        return self._format_response(collabs, format=format)

    def project_collaborator_remove(self, ident, userid, format=None):
        prec = self.ident_record("project", ident)
        collabs = self.project_collaborator_list(prec)
        if not isinstance(userid, tuple):
            userid = (userid,)
        missing = set(userid) - set(c["id"] for c in collabs)
        if missing:
            missing = ", ".join(missing)
            raise AEError(f"Collaborator(s) not found: {missing}")
        collabs = [c for c in collabs if c["id"] not in userid]
        return self.project_collaborator_list_set(prec, collabs, format=format)

    def _pre_resource_profile(self, response):
        for profile in response:
            profile["description"], params = profile["description"].rsplit(" (", 1)
            for param in params.rstrip(")").split(", "):
                k, v = param.split(": ", 1)
                profile[k.lower()] = v
            if "gpu" not in profile:
                profile["gpu"] = 0
        return response

    def resource_profile_list(self, filter=None, format=None):
        response = self._get("projects/actions", params={"q": "create_action"})
        response = response[0]["resource_profiles"]
        response = self._fix_records("resource_profile", response, filter=filter)
        return self._format_response(response, format=format)

    def resource_profile_info(self, name, format=None, quiet=False):
        response = self.ident_record("resource_profile", name, quiet)
        return self._format_response(response, format=format)

    def _pre_editor(self, response):
        for rec in response:
            rec["packages"] = ", ".join(rec["packages"])
        return response

    def editor_list(self, filter=None, format=None):
        response = self._get("projects/actions", params={"q": "create_action"})
        response = response[0]["editors"]
        response = self._fix_records("editor", response, filter=filter)
        return self._format_response(response, format=format)

    def editor_info(self, name, format=None, quiet=False):
        response = self.ident_record("editor", name, quiet)
        return self._format_response(response, format=format)

    def _pre_sample(self, records):
        for record in records:
            if record.get("is_default"):
                record["is_default"] = False
        first_template = None
        found_default = False
        for record in records:
            record["is_default"] = bool(not found_default and record.get("is_template") and record.get("is_default"))
            record.setdefault("is_template", False)
            first_template = first_template or record
            found_default = found_default or record["is_default"]
        if not found_default and first_template:
            first_template["is_default"] = True
        return records

    def sample_list(self, filter=None, format=None):
        records = self._get("template_projects") + self._get("sample_projects")
        response = self._fix_records("sample", records, filter)
        return self._format_response(response, format=format)

    def sample_info(self, ident, format=None, quiet=False):
        response = self.ident_record("sample", ident, quiet)
        return self._format_response(response, format=format)

    def sample_clone(self, ident, name=None, tag=None, make_unique=None, wait=True, format=None):
        record = self.ident_record("sample", ident)
        if name is None:
            name = record["name"]
            if make_unique is None:
                make_unique = True
        return self.project_create(
            record["download_url"],
            name=name,
            tag=tag,
            make_unique=make_unique,
            wait=wait,
            format=format,
        )

    def project_sessions(self, ident, format=None):
        id = self.ident_record("project", ident)["id"]
        response = self._get_records(f"projects/{id}/sessions")
        return self._format_response(response, format=format)

    def project_deployments(self, ident, format=None):
        id = self.ident_record("project", ident)["id"]
        response = self._get_records(f"projects/{id}/deployments")
        return self._format_response(response, format=format)

    def project_jobs(self, ident, format=None):
        id = self.ident_record("project", ident)["id"]
        response = self._get_records(f"projects/{id}/jobs")
        return self._format_response(response, format=format)

    def project_runs(self, ident, format=None):
        id = self.ident_record("project", ident)["id"]
        response = self._get_records(f"projects/{id}/runs")
        return self._format_response(response, format=format)

    def project_activity(self, ident, limit=None, all=False, latest=False, format=None):
        id = self.ident_record("project", ident)["id"]
        if all and latest:
            raise AEError("Cannot specify both all=True and latest=True")
        elif limit is None:
            limit = 1 if latest else (0 if all else 10)
        elif all and limit > 0:
            raise AEError(f"Cannot specify both all=True and limit={limit}")
        elif latest and limit > 1:
            raise AEError(f"Cannot specify both latest=True and limit={limit}")
        elif limit <= 0:
            limit = 999999
        api_kwargs = {"params": {"sort": "-updated", "page[size]": limit}}
        response = self._get_records(f"projects/{id}/activity", api_kwargs=api_kwargs)
        if latest:
            response = response[0]
        return self._format_response(response, format=format)

    def _pre_revision(self, records):
        first = True
        for rec in records:
            rec["project_id"] = "a0-" + rec["url"].rsplit("/", 3)[1]
            rec["latest"], first = first, False
            commands = rec["commands"]
            for c in commands:
                c["_record_type"] = "command"
            rec["commands"] = ", ".join(c["id"] for c in commands)
            rec["_commands"] = commands
        return records

    def _post_revision(self, records, project=None):
        for rec in records:
            rec["_project"] = project
        return records

    def _revisions(self, ident, filter=None, latest=False, single=False, quiet=False):
        if isinstance(ident, dict):
            revision = ident.get("_revision")
        elif isinstance(ident, tuple):
            revision = "".join(r[9:] for r in ident if r.startswith("revision="))
            ident = tuple(r for r in ident if not r.startswith("revision="))
        else:
            if isinstance(ident, str):
                ident = Identifier.from_string(ident)
            revision = ident.revision
        if revision == "latest":
            latest = latest or True
            revision = None
        elif revision:
            latest = False
        prec = self.ident_record("project", ident, quiet=quiet)
        if prec is None:
            return None
        id = prec["id"]
        if not filter:
            filter = ()
        if latest:
            filter = (f"latest=True",) + filter
        elif revision and revision != "*":
            filter = (f"name={revision}",) + filter
        response = self._get_records(f"projects/{id}/revisions", filter=filter, project=prec, retry_if_empty=True)
        if latest == "keep" and response:
            response[0]["name"] = "latest"
        if single:
            response = self._should_be_one(response, filter, quiet)
        return response

    def _revision(self, ident, keep_latest=False, quiet=False):
        latest = "keep" if keep_latest else True
        return self._revisions(ident, latest=latest, single=True, quiet=quiet)

    def revision_list(self, ident, filter=None, format=None):
        response = self._revisions(ident, filter, quiet=False)
        return self._format_response(response, format=format)

    def revision_info(self, ident, format=None, quiet=False):
        rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec, format=format)

    def revision_commands(self, ident, format=None, quiet=False):
        rrec = self._revision(ident, quiet=quiet)
        return self._format_response(rrec["_commands"], format=format)

    def project_download(self, ident, filename=None):
        rrec = self._revision(ident, keep_latest=True)
        prec, rev = rrec["_project"], rrec["id"]
        need_filename = not bool(filename)
        if need_filename:
            revdash = f'-{rrec["name"]}' if rrec["name"] != "latest" else ""
            filename = f'{prec["name"]}{revdash}.tar.gz'
        response = self._get(f'projects/{prec["id"]}/revisions/{rev}/archive', format="blob")
        with open(filename, "wb") as fp:
            fp.write(response)
        if need_filename:
            return filename

    def project_image(
        self,
        ident,
        command=None,
        condarc=None,
        dockerfile=None,
        debug=False,
    ):
        """Build docker image"""
        rrec = self._revision(ident, keep_latest=True)
        prec, rev = rrec["_project"], rrec["id"]
        name = prec["name"].replace(" ", "").lower()
        owner = prec["owner"].replace("@", "_at_")
        tag = f"{owner}/{name}:{rev}"

        dockerfile_contents = get_dockerfile(dockerfile)
        condarc_contents = get_condarc(condarc)

        if command:
            commands = [c["id"] for c in rrec["_commands"]]
            if not commands:
                print("There are no configured commands in this project.")
                print("Remove the --command option to build the container anyway.")
                return
            if command in commands:
                dockerfile_contents += f"\nCMD anaconda-project run {command} --anaconda-project-port 8086"
            else:
                print(f"The command {command} is not one of the configured commands.")
                print("Available commands are:")
                for c in rrec["_commands"]:
                    default = c.get("default", False)
                    if default:
                        print(f'  {c["id"]:15s} (default)')
                    else:
                        print(f'  {c["id"]:15s}')
                return
        else:
            default_cmd = [c["id"] for c in rrec["_commands"] if c.get("default")]
            if default_cmd:
                dockerfile_contents += f"\nCMD anaconda-project run {default_cmd[0]} --anaconda-project-port 8086"

        with TemporaryDirectory() as tempdir:
            with open(os.path.join(tempdir, "Dockerfile"), "w") as f:
                f.write(dockerfile_contents)

            with open(os.path.join(tempdir, "condarc"), "w") as f:
                f.write(condarc_contents)

            self.project_download(ident, filename=os.path.join(tempdir, "project.tar.gz"))

            print("Starting image build. This may take several minutes.")
            build_image(tempdir, tag=tag, debug=debug)

    def _wait(self, response):
        index = 0
        id = response.get("project_id", response["id"])
        status = response["action"]
        while not status["done"] and not status["error"]:
            time.sleep(1)
            params = {"sort": "-updated", "page[size]": index + 1}
            activity = self._get(f"projects/{id}/activity", params=params)
            try:
                status = next(s for s in activity["data"] if s["id"] == status["id"])
            except StopIteration:
                index = index + 1
        response["action"] = status

    def project_create(self, url, name=None, tag=None, make_unique=None, wait=True, format=None):
        if not name:
            parts = urllib3.util.parse_url(url)
            name = basename(parts.path).split(".", 1)[0]
            if make_unique is None:
                make_unique = True
        params = {"name": name, "source": url, "make_unique": bool(make_unique)}
        if tag:
            params["tag"] = tag
        response = self._post_record("projects", api_kwargs={"json": params})
        if response.get("error"):
            raise RuntimeError("Error creating project: {}".format(response["error"]["message"]))
        if wait:
            self._wait(response)
        if response["action"]["error"]:
            raise RuntimeError("Error processing creation: {}".format(response["action"]["message"]))
        if wait:
            return self.project_info(response["id"], format=format, retry=True)

    def project_upload(self, project_archive, name, tag, wait=True, format=None):
        if not name:
            if type(project_archive) == bytes:
                raise RuntimeError("Project name must be supplied for binary input")
            name = basename(abspath(project_archive))
            for suffix in (
                ".tar.gz",
                ".tar.bz2",
                ".tar.gz",
                ".zip",
                ".tgz",
                ".tbz",
                ".tbz2",
                ".tz2",
                ".txz",
            ):
                if name.endswith(suffix):
                    name = name[: -len(suffix)]
                    break
        try:
            f = None
            if type(project_archive) == bytes:
                f = io.BytesIO(project_archive)
            elif not os.path.exists(project_archive):
                raise RuntimeError(f"File/directory not found: {project_archive}")
            elif not isdir(project_archive):
                f = open(project_archive, "rb")
            elif not isfile(join(project_archive, "anaconda-project.yml")):
                raise RuntimeError(f"Project directory must include anaconda-project.yml")
            else:
                f = io.BytesIO()
                create_tar_archive(project_archive, "project", f)
                project_archive = project_archive + ".tar.gz"
            f.seek(0)
            data = {"name": name}
            if tag:
                data["tag"] = tag
            f = (project_archive, f)
            response = self._post_record(
                "projects/upload",
                record_type="project",
                api_kwargs={"files": {b"project_file": f}, "data": data},
            )
        finally:
            if f is not None:
                f[1].close()
        if response.get("error"):
            raise RuntimeError("Error uploading project: {}".format(response["error"]["message"]))
        if wait:
            self._wait(response)
        if response["action"]["error"]:
            raise RuntimeError("Error processing upload: {}".format(response["action"]["message"]))
        if wait:
            return self.project_info(response["id"], format=format, retry=True)

    def _join_collaborators(self, what, response):
        if isinstance(response, dict):
            what, id = response["_record_type"], response["id"]
            collabs = self._get_records(f"{what}s/{id}/collaborators")
            response["collaborators"] = ", ".join(c["id"] for c in collabs)
            response["_collaborators"] = collabs
        elif response:
            for rec in response:
                self._join_collaborators(what, rec)
        elif hasattr(response, "_columns"):
            response._columns.extend(("collaborators", "_collaborators"))

    def _join_k8s(self, record, changes=False):
        # Maximum number of ids to pass through json body to the k8s endpoint
        k8_s_json_list_max: int = demand_env_var_as_int(name="K8S_JSON_LIST_MAX")

        is_single = isinstance(record, dict)
        rlist = [record] if is_single else record
        if rlist:
            rlist2 = []
            # Limit the size of the input to pod_info to avoid 413 errors
            idchunks = [
                [r["id"] for r in rlist[k : k + k8_s_json_list_max]] for k in range(0, len(rlist), k8_s_json_list_max)
            ]
            record2 = sum((self._k8s("pod_info", ch) for ch in idchunks), [])
            for rec, rec2 in zip(rlist, record2):
                if not rec2:
                    continue
                rlist2.append(rec)
                rec["phase"] = rec2["phase"]
                rec["since"] = rec2["since"]
                rec["rst"] = rec2["restarts"]
                rec["usage/mem"] = rec2["usage"]["mem"]
                rec["usage/cpu"] = rec2["usage"]["cpu"]
                rec["usage/gpu"] = rec2["usage"]["gpu"]
                if changes:
                    if "changes" in rec2:
                        chg = rec2["changes"]
                        chg = ",".join(chg["modified"] + chg["deleted"] + chg["added"])
                        rec["changes"] = chg
                        rec["modified"] = bool(chg)
                    else:
                        rec["modified"] = "n/a"
                        rec["changes"] = ""
                rec["node"] = rec2["node"]
                rec["_k8s"] = rec2
            if not rlist2:
                rlist2 = EmptyRecordList(rlist[0]["_record_type"], rlist[0])
            rlist = rlist2
        if not rlist and hasattr(rlist, "_columns"):
            rlist._columns.extend(("phase", "since", "rst", "usage/mem", "usage/cpu", "usage/gpu"))
            if changes:
                rlist._columns.extend(("changes", "modified"))
            rlist._columns.extend(("node", "_k8s"))
        return record if is_single else rlist

    def _pre_session(self, records):
        # The "name" value in an internal AE5 session record is nothing
        # more than the "id" value with the "a1-" stub removed. Not very
        # helpful, even if understandable.
        precs = {x["id"]: x for x in self._get_records("projects")}
        for rec in records:
            pid = "a0-" + rec["project_url"].rsplit("/", 1)[-1]
            prec = precs.get(pid, {})
            rec["session_name"] = rec["name"]
            rec["name"] = prec["name"]
            rec["project_id"] = pid
            rec["_project"] = prec
        return records

    def _post_session(self, records, k8s=False):
        if k8s:
            return self._join_k8s(records, changes=True)
        return records

    def session_list(self, filter=None, k8s=False, format=None):
        records = self._get_records("sessions", filter, k8s=k8s)
        return self._format_response(records, format, record_type="session")

    def session_info(self, ident, k8s=False, format=None, quiet=False):
        record = self.ident_record("session", ident, quiet=quiet, k8s=k8s)
        return self._format_response(record, format)

    def session_start(
        self,
        ident,
        editor=None,
        resource_profile=None,
        wait=True,
        open=False,
        frame=True,
        format=None,
    ):
        prec = self.ident_record("project", ident)
        id = prec["id"]
        patches = {}
        if editor and prec["editor"] != editor:
            patches["editor"] = editor
        if resource_profile and prec["resource_profile"] != resource_profile:
            patches["resource_profile"] = resource_profile
        if patches:
            self._patch(f"projects/{id}", json=patches)
        response = self._post_record(f"projects/{id}/sessions")
        if response.get("error"):
            raise RuntimeError("Error starting project: {}".format(response["error"]["message"]))
        if wait or open:
            self._wait(response)
        if response["action"].get("error"):
            raise RuntimeError("Error completing session start: {}".format(response["action"]["message"]))
        if open:
            self.session_open(response, frame)
        return self._format_response(response, format=format)

    def session_stop(self, ident):
        id = self.ident_record("session", ident)["id"]
        self._delete(f"sessions/{id}")

    def session_restart(self, ident, wait=True, open=False, frame=True, format=None):
        srec = self.ident_record("session", ident)
        id, pid = srec["id"], srec["project_id"]
        self._delete(f"sessions/{id}")
        # Unlike deployments I am not copying over the editor and resource profile
        # settings from the current session. That's because I want to support the use
        # case where the session settings are patched prior to restart
        return self.session_start(pid, wait=wait, open=open, frame=frame, format=format)

    def session_open(self, ident, frame=True):
        srec = self.ident_record("session", ident)
        if frame:
            scheme, _, hostname, *_, project_id = srec["project_url"].split("/")
            url = f"{scheme}//{hostname}/projects/detail/a0-{project_id}/view"
        else:
            scheme, _, hostname, *_, session_id = srec["url"].split("/")
            url = f"{scheme}//{session_id}.{hostname}/"
        webbrowser.open(url, 1, True)

    def session_changes(self, ident, master=False, format=None):
        id = self.ident_record("session", ident)["id"]
        which = "master" if master else "local"
        result = self._get(f"sessions/{id}/changes/{which}")
        result = self._fix_records("change", result["files"])
        return self._format_response(result, format=format)

    def session_branches(self, ident, format=None):
        id = self.ident_record("session", ident)["id"]
        # Use master because it's more likely to be a smaller result (no changed files)
        result = self._get(f"sessions/{id}/changes/master")
        result = [{"branch": k, "sha1": v} for k, v in result["branches"].items()]
        result = self._fix_records("branch", result)
        return self._format_response(result, format=format)

    def _pre_deployment(self, records):
        # Add the project ID to the deployment record
        for record in [records] if isinstance(records, dict) else records:
            pid = "a0-" + record["project_url"].rsplit("/", 1)[-1]
            record["project_id"] = pid
            if record.get("url"):
                record["endpoint"] = record["url"].split("/", 3)[2].split(".", 1)[0]
        return records

    def _post_deployment(self, records, collaborators=False, k8s=False):
        if collaborators:
            self._join_collaborators("deployments", records)
        if k8s:
            return self._join_k8s(records, changes=False)
        return records

    def deployment_list(self, filter=None, collaborators=False, k8s=False, format=None):
        response = self._get_records("deployments", filter=filter, collaborators=collaborators, k8s=k8s)
        return self._format_response(response, format=format)

    def deployment_info(self, ident, collaborators=False, k8s=False, format=None, quiet=False):
        record = self.ident_record("deployment", ident, collaborators=collaborators, k8s=k8s, quiet=quiet)
        return self._format_response(record, format=format)

    def _pre_endpoint(self, records):
        dlist = self.deployment_list()
        plist = self.project_list()
        dmap = {drec["endpoint"]: drec for drec in dlist if drec["endpoint"]}
        pmap = {prec["id"]: prec for prec in plist}
        newrecs = []
        for rec in records:
            drec = dmap.get(rec["id"])
            if drec:
                rec["name"], rec["deployment_id"] = drec["name"], drec["id"]
                rec["project_url"] = drec["project_url"]
                rec["owner"] = drec["owner"]
                rec["_deployment"] = drec
            else:
                rec["name"], rec["deployment_id"] = "", ""
            rec["project_id"] = "a0-" + rec["project_url"].rsplit("/", 1)[-1]
            prec = pmap.get(rec["project_id"])
            if prec:
                rec["project_name"] = prec["name"]
                rec.setdefault("owner", prec["owner"])
                rec["_project"] = prec
                rec["_record_type"] = "endpoint"
                newrecs.append(rec)
        return newrecs

    def endpoint_list(self, filter=None, format=None):
        response = self._get("/platform/deploy/api/v1/apps/static-endpoints")["data"]
        response = self._fix_records("endpoint", response, filter=filter)
        return self._format_response(response, format=format)

    def endpoint_info(self, ident, format=None):
        response = self.ident_record("endpoint", ident)
        return self._format_response(response, format=format)

    def deployment_collaborator_list(self, ident, filter=None, format=None):
        id = self.ident_record("deployment", ident)["id"]
        response = self._get_records(f"deployments/{id}/collaborators", filter)
        return self._format_response(response, format=format)

    def deployment_collaborator_info(self, ident, userid, format=None, quiet=False):
        filter = f"id={userid}"
        response = self.deployment_collaborator_list(ident, filter=filter)
        response = self._should_be_one(response, filter, quiet)
        return self._format_response(response, format=format)

    def deployment_collaborator_list_set(self, ident, collabs, format=None):
        id = self.ident_record("deployment", ident)["id"]
        result = self._put(f"deployments/{id}/collaborators", json=collabs)
        if result["action"]["error"] or "collaborators" not in result:
            raise AEError(f"Unexpected error adding collaborator: {result}")
        result = self._fix_records("collaborator", result["collaborators"])
        return self._format_response(result, format=format)

    def deployment_collaborator_add(self, ident, userid, group=False, format=None):
        drec = self.ident_record("deployment", ident)
        collabs = self.deployment_collaborator_list(drec)
        ncollabs = len(collabs)
        if not isinstance(userid, tuple):
            userid = (userid,)
        collabs = [c for c in collabs if c["id"] not in userid]
        if len(collabs) != ncollabs:
            self.deployment_collaborator_list_set(drec, collabs)
        collabs.extend({"id": u, "type": "group" if group else "user", "permission": "r"} for u in userid)
        return self.deployment_collaborator_list_set(drec, collabs, format=format)

    def deployment_collaborator_remove(self, ident, userid, format=None):
        drec = self.ident_record("deployment", ident)
        collabs = self.deployment_collaborator_list(drec)
        if not isinstance(userid, tuple):
            userid = (userid,)
        missing = set(userid) - set(c["id"] for c in collabs)
        if missing:
            missing = ", ".join(missing)
            raise AEError(f"Collaborator(s) not found: {missing}")
        collabs = [c for c in collabs if c["id"] not in userid]
        return self.deployment_collaborator_list_set(drec, collabs, format=format)

    def deployment_start(
        self,
        ident,
        name=None,
        endpoint=None,
        command=None,
        resource_profile=None,
        public=False,
        collaborators=None,
        wait=True,
        open=False,
        frame=False,
        stop_on_error=False,
        format=None,
        _skip_endpoint_test=False,
    ):
        rrec = self._revision(ident, keep_latest=True)
        id, prec = rrec["project_id"], rrec["_project"]
        if command is None:
            command = rrec["commands"].split(",", 1)[0]
        if resource_profile is None:
            resource_profile = prec["resource_profile"]
        data = {
            "source": rrec["url"],
            "revision": rrec["name"],
            "resource_profile": resource_profile,
            "command": command,
            "public": bool(public),
            "target": "deploy",
        }
        if name:
            data["name"] = name
        if endpoint:
            if not re.match(r"[A-Za-z0-9-]+", endpoint):
                raise AEError(f"Invalid endpoint: {endpoint}")
            if not _skip_endpoint_test:
                try:
                    self._head(f"/_errors/404.html", subdomain=endpoint)
                    raise AEError('endpoint "{}" is already in use'.format(endpoint))
                except AEUnexpectedResponseError:
                    pass
            data["static_endpoint"] = endpoint
        response = self._post_record(f"projects/{id}/deployments", api_kwargs={"json": data})
        id = response["id"]
        if response.get("error"):
            raise AEError("Error starting deployment: {}".format(response["error"]["message"]))
        if collaborators:
            self.deployment_collaborator_list_set(id, collaborators)
        # The _wait method doesn't work here. The action isn't even updated, it seems
        if wait or stop_on_error:
            while response["state"] in ("initial", "starting"):
                time.sleep(2)
                response = self._get_records(f"deployments/{id}", record_type="deployment")
            if response["state"] != "started":
                if stop_on_error:
                    self.deployment_stop(id)
                raise AEError(f'Error completing deployment start: {response["status_text"]}')
        if open:
            self.deployment_open(response, frame)
        return self._format_response(response, format=format)

    def deployment_restart(self, ident, wait=True, open=False, frame=True, stop_on_error=False, format=None):
        drec = self.ident_record("deployment", ident)
        collab = self.deployment_collaborator_list(drec)
        if drec.get("url"):
            endpoint = drec["url"].split("/", 3)[2].split(".", 1)[0]
            if drec["id"].endswith(endpoint):
                endpoint = None
        else:
            endpoint = None
        self.deployment_stop(drec)
        return self.deployment_start(
            "{}:{}".format(drec["project_id"], drec["revision"]),
            endpoint=endpoint,
            command=drec["command"],
            resource_profile=drec["resource_profile"],
            public=drec["public"],
            collaborators=collab,
            wait=wait,
            open=open,
            frame=frame,
            stop_on_error=stop_on_error,
            format=format,
            _skip_endpoint_test=True,
        )

    def deployment_open(self, ident, frame=False):
        drec = self.ident_record("deployment", ident)
        scheme, _, hostname, _ = drec["project_url"].split("/", 3)
        if frame:
            url = f'{scheme}//{hostname}/deployments/detail/{drec["id"]}/view'
        else:
            url = drec["url"]
        webbrowser.open(url, 1, True)

    def deployment_patch(self, ident, format=None, **kwargs):
        drec = self.ident_record("deployment", ident)
        data = {k: v for k, v in kwargs.items() if v is not None}
        if data:
            id = drec["id"]
            self._patch(f"deployments/{id}", json=data)
            drec = self.ident_record("deployment", id)
        return self._format_response(drec, format=format)

    def deployment_stop(self, ident):
        id = self.ident_record("deployment", ident)["id"]
        self._delete(f"deployments/{id}")

    def deployment_logs(self, ident, which=None, format=None):
        id = self.ident_record("deployment", ident)["id"]
        response = self._get(f"deployments/{id}/logs")
        if which is not None:
            response = response[which]
        return self._format_response(response, format=format)

    def deployment_token(self, request: DeploymentTokenRequest) -> DeploymentTokenResponse:
        id = self.ident_record("deployment", request.ident)["id"]
        response = self._post(f"deployments/{id}/token", format="json")
        return DeploymentTokenResponse.parse_obj(response)

    def _pre_job(self, records):
        precs = {x["id"]: x for x in self._get_records("projects")}
        for rec in records:
            if rec.get("project_url"):
                pid = "a0-" + (rec.get("project_url") or "").rsplit("/", 1)[-1]
                prec = precs.get(pid, {})
                rec["project_id"] = pid
                rec["_project"] = prec
                rec
        return records

    def job_list(self, filter=None, format=None):
        response = self._get_records("jobs", filter=filter)
        return self._format_response(response, format=format)

    def job_info(self, ident, format=None, quiet=False):
        response = self.ident_record("job", ident, quiet=quiet)
        return self._format_response(response, format=format)

    def job_runs(self, ident, format=None):
        id = self.ident_record("job", ident)["id"]
        response = self._get_records(f"jobs/{id}/runs")
        return self._format_response(response, format=format)

    def job_delete(self, ident):
        id = self.ident_record("job", ident)["id"]
        self._delete(f"jobs/{id}")

    def job_pause(self, ident, format=None):
        id = self.ident_record("job", ident)["id"]
        response = self._post_record(f"jobs/{id}/pause", record_type="job")
        return self._format_response(response, format=format)

    def job_unpause(self, ident, format=format):
        id = self.ident_record("job", ident)["id"]
        response = self._post_record(f"jobs/{id}/unpause", record_type="job")
        return self._format_response(response, format=format)

    def job_create(
        self,
        ident,
        schedule=None,
        name=None,
        command=None,
        resource_profile=None,
        variables=None,
        run=None,
        wait=None,
        cleanup=False,
        make_unique=None,
        show_run=False,
        format=None,
    ):
        if run is None:
            run = not schedule or cleanup
        if wait is None:
            wait = cleanup
        if cleanup and schedule:
            raise ValueError("cannot use cleanup=True with a scheduled job")
        if cleanup and (not run or not wait):
            raise ValueError("must specify run=wait=True with cleanup=True")
        rrec = self._revision(ident, keep_latest=True)
        prec, id = rrec["_project"], rrec["project_id"]
        if not command:
            command = rrec["commands"][0]["id"]
        if not resource_profile:
            resource_profile = rrec["_project"]["resource_profile"]
        # AE5's default name generator unfortunately uses colons
        # in the creation of its job names which causes confusion for
        # ae5-tools, which uses them to mark a revision identifier.
        # Furthermore, creating a job with the same name as an deleted
        # job that still has run listings causes an error.
        if not name:
            name = f'{command}-{prec["name"]}'
            if make_unique is None:
                make_unique = True
        if make_unique:
            jnames = {j["name"] for j in self._get(f"jobs")}
            jnames.update(j["name"] for j in self._get(f"runs"))
            if name in jnames:
                bname = name
                for counter in range(1, len(jnames) + 1):
                    name = f"{bname}-{counter}"
                    if name not in jnames:
                        break
        data = {
            "source": rrec["url"],
            "resource_profile": resource_profile,
            "command": command,
            "target": "deploy",
            "schedule": schedule,
            "autorun": run,
            "revision": rrec["name"],
            "name": name,
        }
        if variables:
            data["variables"] = variables
        response = self._post_record(f"projects/{id}/jobs", api_kwargs={"json": data})
        if response.get("error"):
            raise AEError("Error starting job: {}".format(response["error"]["message"]))
        if run:
            jid = response["id"]
            run = self._get_records(f"jobs/{jid}/runs")[-1]
            if wait:
                rid = run["id"]
                while run["state"] not in ("completed", "error", "failed"):
                    time.sleep(5)
                    run = self._get(f"runs/{rid}")
                if cleanup:
                    self._delete(f"jobs/{jid}")
            if show_run:
                response = run
        return self._format_response(response, format=format)

    def job_patch(
        self,
        ident,
        name=None,
        command=None,
        schedule=None,
        resource_profile=None,
        variables=None,
        format=None,
    ):
        jrec = self.ident_record("job", ident)
        id = jrec["id"]
        data = {}
        if name and name != jrec["name"]:
            data["name"] = name
        if command and command != jrec["command"]:
            data["command"] = command
        if schedule and schedule != jrec["schedule"]:
            data["schedule"] = schedule
        if resource_profile and resource_profile != jrec["resource_profile"]:
            data["resource_profile"] = resource_profile
        if variables is not None and data["variables"] != jrec["variables"]:
            data["variables"] = variables
        if data:
            # TODO: This doesn't seem to exist ...
            self._patch_record(f"jobs/{id}", json=data)
            jrec = self.ident_record("job", id)
        return self._format_response(jrec, format=format)

    # runs need the same preprocessing as jobs,
    # and the same postprocessing as sessions
    _pre_run = _pre_job
    _post_run = _post_session

    def run_list(self, k8s=False, filter=None, format=None):
        response = self._get_records("runs", k8s=k8s, filter=filter)
        return self._format_response(response, format=format)

    def run_info(self, ident, k8s=False, format=None, quiet=False):
        response = self.ident_record("run", ident, k8s=k8s, quiet=quiet)
        return self._format_response(response, format=format)

    def run_log(self, ident):
        id = self.ident_record("run", ident)["id"]
        response = self._get(f"runs/{id}/logs")["job"]
        return response

    def run_stop(self, ident, format=None):
        id = self.ident_record("run", ident)["id"]
        response = self._post(f"runs/{id}/stop")
        return self._format_response(response, format=format)

    def run_delete(self, ident):
        id = self.ident_record("run", ident)["id"]
        self._delete(f"runs/{id}")

    def _pre_pod(self, records):
        result = []
        for rec in records:
            if "project_id" in rec:
                type = rec["_record_type"]
                value = {k: rec[k] for k in ("name", "owner", "resource_profile", "id", "project_id")}
                value["type"] = type
                result.append(rec)
        return result

    def _post_pod(self, records):
        return self._join_k8s(records, changes=True)

    def pod_list(self, filter=None, format=None):
        records = self.session_list(filter=filter) + self.deployment_list(filter=filter) + self.run_list(filter=filter)
        records = self._fix_records("pod", records)
        return self._format_response(records, format=format)

    def pod_info(self, pod, format=None, quiet=False):
        record = self.ident_record("pod", pod, quiet=quiet)
        return self._format_response(record, format=format)

    def node_list(self, filter=None, format=None):
        result = []
        for rec in self._k8s("node_info"):
            result.append(
                {
                    "name": rec["name"],
                    "role": rec["role"],
                    "ready": rec["ready"],
                    "capacity/pod": rec["capacity"]["pods"],
                    "capacity/mem": rec["capacity"]["mem"],
                    "capacity/cpu": rec["capacity"]["cpu"],
                    "capacity/gpu": rec["capacity"]["gpu"],
                    "usage/pod": rec["total"]["pods"],
                    "usage/mem": rec["total"]["usage"]["mem"],
                    "usage/cpu": rec["total"]["usage"]["cpu"],
                    "usage/gpu": rec["total"]["usage"]["gpu"],
                    "sessions/pod": rec["sessions"]["pods"],
                    "sessions/mem": rec["sessions"]["usage"]["mem"],
                    "sessions/cpu": rec["sessions"]["usage"]["cpu"],
                    "sessions/gpu": rec["sessions"]["usage"]["gpu"],
                    "deployments/pod": rec["deployments"]["pods"],
                    "deployments/mem": rec["deployments"]["usage"]["mem"],
                    "deployments/cpu": rec["deployments"]["usage"]["cpu"],
                    "deployments/gpu": rec["deployments"]["usage"]["gpu"],
                    "middleware/pod": rec["middleware"]["pods"],
                    "middleware/mem": rec["middleware"]["usage"]["mem"],
                    "middleware/cpu": rec["middleware"]["usage"]["cpu"],
                    "system/pod": rec["system"]["pods"],
                    "system/mem": rec["system"]["usage"]["mem"],
                    "system/cpu": rec["system"]["usage"]["cpu"],
                    "_k8s": rec,
                    "_record_type": "node",
                }
            )
        result = self._fix_records("node", result, filter)
        return self._format_response(result, format=format)

    def node_info(self, node, format=None, quiet=False):
        record = self.ident_record("node", node, quiet=quiet)
        return self._format_response(record, format=format)
