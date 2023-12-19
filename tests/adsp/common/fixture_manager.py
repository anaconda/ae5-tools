from __future__ import annotations

import json
import logging
import os
import time
from copy import copy, deepcopy

from ae5_tools import AEAdminSession, AEException, AEUnexpectedResponseError, AEUserSession, demand_env_var

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FixtureManager:
    def __init__(self, ae_admin_session: AEAdminSession | None = None) -> None:
        self.ae_admin_session: AEAdminSession = ae_admin_session if ae_admin_session else FixtureManager.build_session(admin=True)
        self.accounts: list[dict] = []
        self.projects: list[dict] = []

    def __del__(self):
        self.destroy_fixture_projects(ignore_error=True)
        self.destroy_fixture_accounts()

    def load(self, state: str, remove: bool = False) -> None:
        with open(file=state, mode="r", encoding="utf-8") as file:
            partial_state: dict = json.load(file)
        self.accounts = []
        self.projects = []

        partial_accounts: list[dict] = partial_state["accounts"]
        partial_projects: list[dict] = partial_state["projects"]

        self.accounts = partial_accounts
        self.create_fixture_connections()

        self.projects = partial_projects

        if remove:
            os.unlink(path=state)

    @staticmethod
    def _resolve_conn_params(
        hostname: str | None = None, username: str | None = None, password: str | None = None, admin: bool = False
    ) -> tuple[str, str, str]:
        hostname = hostname if hostname else demand_env_var(name="AE5_HOSTNAME")

        if not username:
            if admin:
                username = demand_env_var(name="AE5_ADMIN_USERNAME")
            else:
                username = demand_env_var(name="AE5_USERNAME")

        if not password:
            if admin:
                password = demand_env_var(name="AE5_ADMIN_PASSWORD")
            else:
                password = demand_env_var(name="AE5_PASSWORD")

        return hostname, username, password

    @staticmethod
    def build_session(
        hostname: str | None = None, username: str | None = None, password: str | None = None, admin: bool = False
    ) -> AEUserSession | AEAdminSession:
        params: tuple = FixtureManager._resolve_conn_params(hostname=hostname, username=username, password=password, admin=admin)
        if admin:
            return AEAdminSession(*params)
        return AEUserSession(*params)

    def create_fixture_accounts(self, accounts: list, force: bool = False) -> None:
        local_accounts: list = deepcopy(accounts)
        while len(local_accounts) > 0:
            retry: bool = True
            account = local_accounts.pop()
            while retry:
                try:
                    self.ae_admin_session.user_create(
                        username=account["username"],
                        email=account["email"],
                        firstname=account["firstname"],
                        lastname=account["lastname"],
                        password=account["password"],
                        email_verified=True,
                        enabled=True,
                        password_temporary=False,
                    )
                    self.accounts.append(account)
                    logger.info(f"User account {account['username']} created.")
                    retry = False
                except AEUnexpectedResponseError as error:
                    if "Unexpected response: 409 Conflict" in str(error):
                        if force:
                            # remove, and retry.
                            logger.warning("User account {account['username']} already exists, removing..")
                            self._destroy_account(username=account["username"])
                        else:
                            logger.warning(f"User account {account['username']} already exists, will not [re]create (or remove).")
                            self.accounts.append(account)
                            retry = False
                    else:
                        raise error from error

    def create_fixture_connections(self) -> None:
        for account in self.accounts:
            self._create_fixture_conn(username=account["username"])

    def _create_fixture_conn(self, username: str) -> None:
        account: dict = [user for user in self.accounts if user["username"] == username][0]
        if "conn" in account:
            logger.warning(f"User account {username} already has an active connection, skipping ...")
        else:
            logger.info(f"Creating connection for user {account['username']}")
            account["conn"] = FixtureManager.build_session(
                hostname=self.ae_admin_session.hostname,
                username=account["username"],
                password=account["password"],
                admin=False,
            )

    def destroy_fixture_accounts(self) -> None:
        while len(self.accounts) > 0:
            account: dict = self.accounts.pop()
            if account["conn"]:
                logger.info(f"Disconnecting user {account['username']}")
                try:
                    account["conn"].disconnect()
                except AEException as error:
                    if "Invalid username or password." in str(error):
                        # Most likely the account has already been deleted.
                        logger.warning(str(error))
                        logger.warning("Most likely the account has already been deleted.")
                        pass
                    else:
                        logger.error(str(error))
                        raise error from error
                account["conn"] = None
            self._destroy_account(username=account["username"])

    def destroy_fixture_projects(self, ignore_error: bool = False) -> None:
        for project in self.projects:
            try:
                self._destroy_fixture_project(name=project["record"]["name"], owner=project["record"]["owner"])
            except AEException as error:
                if ignore_error:
                    logger.warning(str(error))
                    if "Invalid username or password." in str(error):
                        pass
                    else:
                        logger.error(str(error))
                        raise error from error
                else:
                    logger.error(str(error))
                    raise error from error

    def _destroy_account(self, username: str) -> None:
        if username == self.ae_admin_session.username:
            raise Exception("Will not delete self")
        else:
            try:
                self.ae_admin_session.user_delete(username=username)
                logger.info(f"User account {username} deleted.")
            except AEException as error:
                msg: str = error.args[0]
                if msg == f"No records found matching username={username}|id={username}":
                    logger.warning(f"No user found matching username={username}|id={username}, skipping removal.")
                else:
                    raise error from error

    def upload_fixture_project(self, proj_params: dict, owner: str, force: bool = False):
        logger.info(f"Uploading project {proj_params['name']} for account {owner}")
        conn: AEUserSession = self.get_account_conn(username=owner)  # [user for user in self.accounts if user["username"] == owner][0]["conn"]

        # {'git_repos': {}, 'repository': 'tooltest1-74ae9699eea84681ae49c8beb4d3ae58', 'editor': 'jupyterlab', 'owner': 'tooltest1', 'tags': [], 'repo_owned': True, 'name': 'testproj1', 'repo_url': 'http://anaconda-enterprise-ap-git-storage/anaconda/tooltest1-74ae9699eea84681ae49c8beb4d3ae58.git', 'created': '2023-12-14T17:36:26.094764+00:00', 'git_server': 'default', 'project_create_status': 'done', 'url': 'http://anaconda-enterprise-ap-storage/projects/74ae9699eea84681ae49c8beb4d3ae58', 'updated': '2023-12-14T17:36:26.094764+00:00', 'id': 'a0-74ae9699eea84681ae49c8beb4d3ae58', 'resource_profile': 'default', '_record_type': 'project'}

        retry: bool = True
        while retry:
            try:
                response: dict = conn.project_upload(
                    project_archive=proj_params["artifact"], name=proj_params["name"], tag=proj_params["tag"], wait=True
                )
                proj: dict = deepcopy(proj_params)
                proj["record"] = response
                self.projects.append(proj)
                retry = False
            except AEUnexpectedResponseError as error:
                if "Unexpected response: 400 Project name is not unique" in str(error):
                    if force:
                        logger.warning("Enforcing wait after encountering error on project upload")
                        time.sleep(30)
                        # delete, and then allow it to loop ...
                        logger.warning(f"Project {proj_params['name']} for account {owner} already exists, forcibly deleting ..")
                        self._destroy_fixture_project(name=proj_params["name"], owner=owner)
                    else:
                        logger.warning(f"Project {proj_params['name']} for account {owner} already exists, pulling project info ..")
                        response: dict = conn.project_info(ident=f"{owner}/{proj_params['name']}")
                        proj: dict = deepcopy(proj_params)
                        proj["record"] = response
                        self.projects.append(proj)
                        retry = False
                else:
                    raise error from error

    def _lookup_fixture(self, name: str, owner: str) -> dict | None:
        for project in self.projects:
            if owner == project["record"]["owner"] and name == project["record"]["name"]:
                return project

    def _unmanage_fixture(self, name: str, owner: str) -> None:
        new_projects = [project for project in self.projects if project["record"]["owner"] != owner and project["record"]["name"] != name]
        self.projects = new_projects

    def _destroy_fixture_project(self, name: str, owner: str) -> None:
        # Ensure fixture is managed
        if not self._lookup_fixture(name=name, owner=owner):
            logger.warning(f"Unable to find managed project fixture for project {name} for owner {owner}, skipping removal..")
            return

        conn: AEUserSession = self.get_account_conn(username=owner)

        retry: bool = True
        while retry:
            if self._does_project_exist(name=name, owner=owner):
                """"""
                try:
                    logger.info(f"Deleting project {name} for account {owner}")
                    conn.project_delete(ident=f"{owner}/{name}")
                    self._unmanage_fixture(name=name, owner=owner)
                except AEException as error:
                    if f"No projects found matching name={name}" in str(error):
                        # then we are out of sync ..
                        logger.info("Project state is out of sync, enforced wait before retry")
                        time.sleep(30)
                    else:
                        raise error from error
                except Exception as error:
                    print("unhandled exception")
                    print(type(error))
                    print(str(error))
                    raise error from error
            else:
                retry = False

    def get_account_conn(self, username: str) -> AEUserSession:
        return [user for user in self.accounts if user["username"] == username][0]["conn"]

    def _does_project_exist(self, name: str, owner: str) -> bool:
        conn: AEUserSession = self.get_account_conn(username=owner)
        try:
            conn.project_info(ident=f"{owner}/{name}")
            return True
        except AEException as error:
            if "No projects found matching name" in str(error):
                return False
            else:
                raise error from error
        except Exception as error:
            print("unhandled exception")
            print(type(error))
            print(str(error))
            raise error from error

    def __str__(self) -> str:
        partial: dict = {"accounts": [], "projects": self.projects}
        for account in self.accounts:
            new_account: dict = copy(account)  # shallow
            del new_account["conn"]
            partial["accounts"].append(new_account)

        return json.dumps(partial, indent=4)
