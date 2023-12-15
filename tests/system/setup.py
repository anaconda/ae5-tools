# Configure our environment
# 1. Create test accounts: 1,2,3
# 2. Upload projects 1,2,3 -> user accounts 1,2,3
# 3. Set expected fixture attributes (tool, sharing)
# 4. Execute test harness
# 5. cleanup
from __future__ import annotations

import json
import logging
import os
import time
from copy import deepcopy
from pathlib import Path

from ae5_tools import demand_env_var
from ae5_tools.api import AEAdminSession, AEException, AEUnexpectedResponseError, AEUserSession

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FixtureFactory:
    @staticmethod
    def resolve_conn_params(
        hostname: str | None = None, username: str | None = None, password: str | None = None
    ) -> tuple[str, str, str]:
        hostname: str = hostname if hostname else demand_env_var(name="AE5_HOSTNAME")
        username: str = username if username else demand_env_var(name="AE5_ADMIN_USERNAME")
        password: str = password if password else demand_env_var(name="AE5_ADMIN_PASSWORD")

        return hostname, username, password

    @staticmethod
    def get_realm_admin_session(
        hostname: str | None = None, username: str | None = None, password: str | None = None
    ) -> AEAdminSession:
        hostname, username, password = FixtureFactory.resolve_conn_params(
            hostname=hostname, username=username, password=password
        )

        # Create the session directly and provide the AE5 config and credentials:
        ae_admin_session: AEAdminSession = AEAdminSession(
            *FixtureFactory.resolve_conn_params(hostname=hostname, username=username, password=password)
        )

        return ae_admin_session

    @staticmethod
    def get_user_session(
        hostname: str | None = None, username: str | None = None, password: str | None = None
    ) -> AEUserSession:
        hostname, username, password = FixtureFactory.resolve_conn_params(
            hostname=hostname, username=username, password=password
        )

        # Create the session directly and provide the AE5 config and credentials:
        ae_user_session: AEUserSession = AEUserSession(
            *FixtureFactory.resolve_conn_params(hostname=hostname, username=username, password=password)
        )
        return ae_user_session


class FixtureManager:
    def __init__(self, ae_admin_session: AEAdminSession) -> None:
        self.ae_admin_session: AEAdminSession = ae_admin_session
        self.accounts: list[dict] = []
        self.projects: list[dict] = []

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
                            logger.warning(
                                f"User account {account['username']} already exists, will not [re]create (or remove)."
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
        if account["conn"]:
            logger.warning(f"User account {username} already has an active connection, skipping ...")
        else:
            logger.info(f"Creating connection for user {account['username']}")
            account["conn"] = FixtureFactory.get_user_session(
                hostname=self.ae_admin_session.hostname, username=account["username"], password=account["password"]
            )

    def destroy_fixture_accounts(self) -> None:
        while len(self.accounts) > 0:
            account: dict = self.accounts.pop()
            if account["conn"]:
                logger.info(f"Disconnecting  user {account['username']}")
                account["conn"].disconnect()
                account["conn"] = None
            self._destroy_account(username=account["username"])

    def destroy_fixture_projects(self) -> None:
        while len(self.projects) > 0:
            project: dict = self.projects.pop()
            self._destroy_fixture_project(name=project["record"]["name"], owner=project["record"]["owner"])

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

    def _upload_fixture_project(self, proj_params: dict, owner: str, force: bool = False):
        logger.info(f"Uploading project {proj_params['name']} for account {owner}")
        conn: AEUserSession = self._get_account_conn(
            username=owner
        )  # [user for user in self.accounts if user["username"] == owner][0]["conn"]

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
                        logger.warning(
                            f"Project {proj_params['name']} for account {owner} already exists, forcibly deleting .."
                        )
                        self._destroy_fixture_project(name=proj_params["name"], owner=owner)
                    else:
                        logger.warning(
                            f"Project {proj_params['name']} for account {owner} already exists, pulling project info .."
                        )
                        response: dict = conn.project_info(ident=f"{owner}/{proj_params['name']}")
                        proj: dict = deepcopy(proj_params)
                        proj["record"] = response
                        self.projects.append(proj)
                        retry = False
                else:
                    raise error from error

    def _destroy_fixture_project(self, name: str, owner: str) -> None:
        conn: AEUserSession = self._get_account_conn(username=owner)

        retry: bool = True
        while retry:
            if self._does_project_exist(name=name, owner=owner):
                """"""
                try:
                    logger.info(f"Deleting project {name} for account {owner}")
                    conn.project_delete(ident=f"{owner}/{name}")
                    # logger.info("Enforced wait after project removal")
                    #
                    # # # TODO: remove from self.projects...
                    # # for project in self.projects:
                    # #     if project[""]
                    #
                    # time.sleep(30)
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

    def _get_account_conn(self, username: str) -> AEUserSession:
        return [user for user in self.accounts if user["username"] == username][0]["conn"]

    def _does_project_exist(self, name: str, owner: str) -> bool:
        conn: AEUserSession = self._get_account_conn(username=owner)
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


if __name__ == "__main__":
    force: bool = False

    config: dict = {
        "service_accounts": [
            {
                "username": "tooltest1",
                "email": "tooltest1@localhost.local",
                "firstname": "tooltest",
                "lastname": "1",
                "password": "tooltest1",
                "conn": None,
            },
            {
                "username": "tooltest2",
                "email": "tooltest2@localhost.local",
                "firstname": "tooltest",
                "lastname": "2",
                "password": "tooltest2",
                "conn": None,
            },
            {
                "username": "tooltest3",
                "email": "tooltest3@localhost.local",
                "firstname": "tooltest",
                "lastname": "3",
                "password": "tooltest3",
                "conn": None,
            },
        ],
        "projects": [
            {"name": "testproj1", "artifact": "tests/fixtures/system/testproj1.tar.gz", "tag": "0.1.0"},
            {"name": "testproj2", "artifact": "tests/fixtures/system/testproj2.tar.gz", "tag": "0.1.0"},
            {"name": "testproj3", "artifact": "tests/fixtures/system/testproj3.tar.gz", "tag": "0.1.0"},
        ],
    }

    factory: FixtureFactory = FixtureFactory()
    manager: FixtureManager = FixtureManager(ae_admin_session=FixtureFactory.get_realm_admin_session())

    # Create service accounts (and connections)
    manager.create_fixture_accounts(accounts=config["service_accounts"], force=force)
    manager.create_fixture_connections()

    # 1. Each user gets all three projects.
    for account in config["service_accounts"]:
        for proj in config["projects"]:
            manager._upload_fixture_project(proj_params=proj, owner=account["username"], force=force)

    # 2. Build our relationships.
    logger.info("Building project / account relationships")

    # User 3 shares all projects with User 1
    source_user_conn: AEUserSession = manager._get_account_conn(username=config["service_accounts"][2]["username"])
    target_user_name: str = config["service_accounts"][0]["username"]

    for project in manager.projects:
        if project["record"]["owner"] == config["service_accounts"][2]["username"]:
            project_id: str = project["record"]["id"]
            response = source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

    # 3. Set editors for user 1's projects
    source_user_conn: AEUserSession = manager._get_account_conn(username=config["service_accounts"][0]["username"])
    for project in manager.projects:
        if project["record"]["owner"] == config["service_accounts"][0]["username"]:
            project_name: str = project["record"]["name"]
            project_id: str = project["record"]["id"]
            logger.info(
                f"Setting default editor on project {project['record']['name']} for {project['record']['owner']}"
            )

            if project_name == config["projects"][0]["name"]:
                source_user_conn.project_patch(ident=project_id, editor="vscode")  # jupyterlab, notebook, vscode
            elif project_name == config["projects"][1]["name"]:
                source_user_conn.project_patch(ident=project_id, editor="notebook")  # jupyterlab, notebook, vscode
            elif project_name == config["projects"][2]["name"]:
                source_user_conn.project_patch(ident=project_id, editor="jupyterlab")  # jupyterlab, notebook, vscode
            else:
                raise NotImplementedError("Unknown project to update default editor on")

    # print(manager.projects)
    # print(manager.accounts)

    # Tear down tests
    # manager.destroy_fixture_projects()
    # manager.destroy_fixture_accounts()
