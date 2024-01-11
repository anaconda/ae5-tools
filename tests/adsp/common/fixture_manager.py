from __future__ import annotations

import json
import logging
import time
from abc import abstractmethod
from copy import copy, deepcopy

from ae5_tools import AEAdminSession, AEException, AEUnexpectedResponseError, AEUserSession, demand_env_var, get_env_var

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FixtureManager:
    def __init__(self, config: dict | None = None, ae_admin_session: AEAdminSession | None = None) -> None:
        self.accounts: list[dict] = []
        self.projects: list[dict] = []
        self.sessions: list[dict] = []  # not connections, but user sessions within adsp
        self.config: dict = config
        self.ae_admin_session: AEAdminSession = ae_admin_session if ae_admin_session else FixtureManager.build_session(admin=True)

    def __del__(self):
        if "teardown" in self.config and self.config["teardown"]:
            self.destroy_fixture_projects(ignore_error=True)
            self.destroy_fixture_accounts()

    @abstractmethod
    def _setup(self) -> None:
        """ """

    def __enter__(self) -> FixtureManager:
        self._setup()
        return self

    def __exit__(self, type, value, traceback):
        self.__del__()

    def _get_account(self, id: str) -> dict:
        return [account for account in self.accounts if account["id"] == id][0]

    def load(self, state: str) -> None:
        with open(file=state, mode="r", encoding="utf-8") as file:
            partial_state: dict = json.load(file)

        self.projects = partial_state["projects"]
        self.config = partial_state["config"]
        self.accounts = partial_state["accounts"]
        self.create_fixture_connections()

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
                    logger.info("User account %s created.", account["username"])
                    retry = False
                except AEUnexpectedResponseError as error:
                    if "Unexpected response: 409 Conflict" in str(error):
                        if force:
                            # remove, and retry.
                            logger.warning("User account %s already exists, removing..", account["username"])
                            self._destroy_account(username=account["username"])
                        else:
                            logger.warning(
                                "User account %s already exists, will not [re]create (or remove). Password may be incorrect..", account["username"]
                            )
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
            logger.warning("User account %s already has an active connection, skipping ...", username)
        else:
            if "password" in account:
                logger.info("Creating connection for user %s", account["username"])
                account["conn"] = FixtureManager.build_session(
                    hostname=self.ae_admin_session.hostname,
                    username=account["username"],
                    password=account["password"],
                    admin=False,
                )
            else:
                logger.warning("Unable to create connection for user %s, no password specified!", account["username"])

    def destroy_fixture_accounts(self) -> None:
        while len(self.accounts) > 0:
            account: dict = self.accounts.pop()
            if account["conn"]:
                logger.info("Disconnecting user %s", account["username"])
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
        local_projects: list[dict] = deepcopy(self.projects)
        for project in local_projects:
            retry: bool = True
            while retry:
                try:
                    if "record" in project:
                        self._destroy_fixture_project(name=project["record"]["name"], owner=project["record"]["owner"], force=self.config["force"])
                    retry = False
                except AEException as error:
                    if ignore_error:
                        logger.warning(str(error))
                        if "Invalid username or password." in str(error):
                            retry = False
                        else:
                            logger.error(str(error))
                    else:
                        logger.error(str(error))
                    time.sleep(5)

    def _destroy_account(self, username: str) -> None:
        if username == self.ae_admin_session.username:
            raise Exception("Will not delete self")
        else:
            try:
                self.ae_admin_session.user_delete(username=username)
                logger.info("User account %s deleted.", username)
            except AEException as error:
                msg: str = error.args[0]
                if msg == f"No records found matching username={username}|id={username}":
                    logger.warning("No user found matching username=%s|id=%s, skipping removal.", username, username)
                else:
                    raise error from error

    def upload_fixture_project(self, proj_params: dict, owner: str, force: bool = False):
        conn: AEUserSession = self.get_account_conn(username=owner)

        retry: bool = True
        while retry:
            logger.info("Uploading project %s for account %s ..", proj_params["name"], owner)
            try:
                response: dict = conn.project_upload(
                    project_archive=proj_params["artifact"], name=proj_params["name"], tag=proj_params["tag"], wait=True
                )
                proj: dict = deepcopy(proj_params)
                proj["record"] = response
                self.projects.append(proj)
                retry = False
            except AEUnexpectedResponseError as error:
                if "Project name is not unique" in str(error):
                    if force:
                        # delete, and then allow it to loop ...
                        logger.warning("Project %s for account %s already exists, forcibly deleting ..", proj_params["name"], owner)
                        time.sleep(2)
                        self._destroy_fixture_project(name=proj_params["name"], owner=owner, force=force)
                    else:
                        logger.warning("Project %s for account %s already exists, pulling project info ..", proj_params["name"], owner)
                        response: dict = conn.project_info(ident=f"{owner}/{proj_params['name']}")
                        proj: dict = deepcopy(proj_params)
                        proj["record"] = response
                        self.projects.append(proj)
                        retry = False
                else:
                    raise error from error

    def _lookup_fixture(self, name: str, owner: str) -> dict | None:
        for project in self.projects:
            if "record" in project:
                if owner == project["record"]["owner"] and name == project["record"]["name"]:
                    return project

    def _unmanage_fixture(self, name: str, owner: str) -> None:
        for project in self.projects:
            if "record" in project:
                if project["record"]["owner"] == owner and project["record"]["name"] == name:
                    self.projects.remove(project)

    def _destroy_fixture_project(self, name: str, owner: str, force: bool) -> None:
        # Ensure fixture is managed
        if not force and not self._lookup_fixture(name=name, owner=owner):
            logger.warning("Unable to find managed project fixture for project %s for owner %s, skipping removal..", name, owner)
            logger.warning(self.projects)
            return

        conn: AEUserSession = self.get_account_conn(username=owner)

        retry: bool = True
        while retry:
            if self._does_project_exist(name=name, owner=owner):
                """"""
                try:
                    logger.info("Deleting project %s for account %s ..", name, owner)
                    conn.project_delete(ident=f"{owner}/{name}")
                    self._unmanage_fixture(name=name, owner=owner)
                    time.sleep(10)
                except AEException as error:
                    if f"No projects found matching name={name}" in str(error):
                        # then we are out of sync ..
                        logger.info("Project state is out of sync, enforced wait before retry")
                        time.sleep(30)
                    else:
                        raise error from error
                except Exception as error:
                    logger.error("unhandled exception")
                    logger.error(type(error))
                    logger.error(str(error))
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
            logger.error("unhandled exception")
            logger.error(type(error))
            logger.error(str(error))
            raise error from error

    def __str__(self) -> str:
        partial: dict = {"config": self.config, "accounts": [], "projects": self.projects}
        for account in self.accounts:
            new_account: dict = copy(account)  # shallow
            if "conn" in new_account:
                del new_account["conn"]
            partial["accounts"].append(new_account)

        return json.dumps(partial, indent=4)
