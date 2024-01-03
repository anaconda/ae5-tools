""" """

from __future__ import annotations

import json
import logging
import uuid

from dotenv import load_dotenv

from ae5_tools import AEUserSession, demand_env_var_as_bool, get_env_var
from tests.adsp.common.fixture_manager import FixtureManager
from tests.adsp.common.utils import _process_launch_wait

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    shell_out_cmd: str = "python -m pytest --cov=ae5_tools --show-capture=all -rP tests/system/ae5_tools --cov-append --cov-report=xml -vv"

    if get_env_var(name="CI") and demand_env_var_as_bool(name="CI"):
        shell_out_cmd += " --ci-skip"

    _process_launch_wait(shell_out_cmd=shell_out_cmd)


class SystemTestFixtureSuite(FixtureManager):
    """
    System Test Setup
        1. Environment Setup
            A. Create test accounts: tooltest, tooltest2, tooltest3
            B. Upload projects 1,2,3 -> user accounts 1,2,3
            C. Set expected fixture attributes (tool, sharing)
        3. (Optional) Environment Teardown

        This covers the current suite of system tests.
        Tests which need additions to this are expected to manage the lifecycle of those effects.
    """

    def _setup(self) -> None:
        # Create Fixtures

        self._create_service_accounts()
        self._upload_projects()
        self._build_relationships()
        self._set_project_properties()

    def _create_service_accounts(self):
        # Create service accounts (and connections)
        self.create_fixture_accounts(accounts=self.config["accounts"], force=self.config["force"])
        self.create_fixture_connections()

    def _upload_projects(self):
        # 1. Each user gets all three projects.
        for account in self.config["accounts"]:
            for proj in self.config["projects"]:
                self.upload_fixture_project(proj_params=proj, owner=account["username"], force=self.config["force"])

    def _build_relationships(self):
        # 2. Build our relationships.
        logger.info("Building project / account relationships")

        # User 3 shares projects 1 & 2 with User 1
        source_user = self._get_account(id="3")
        source_user_conn: AEUserSession = self.get_account_conn(username=source_user["username"])
        target_user_name: str = self._get_account(id="1")["username"]

        for project in self.projects:
            if project["record"]["owner"] == source_user["username"] and project["record"]["name"] in [
                "testproj1",
                "testproj2",
            ]:
                project_id: str = project["record"]["id"]
                source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

        # User 1 shares projects to different numbers of users
        source_user_conn: AEUserSession = self.get_account_conn(username=self._get_account(id="1")["username"])
        for project in self.projects:
            if project["record"]["owner"] == self._get_account(id="1")["username"]:
                project_name: str = project["record"]["name"]
                project_id: str = project["record"]["id"]
                logger.info(f"Configuring sharing on project {project['record']['name']} for {project['record']['owner']}")

                if project_name == self.config["projects"][0]["name"]:
                    # Add user 2
                    target_user_name: str = self._get_account(id="2")["username"]
                    source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)
                elif project_name == self.config["projects"][1]["name"]:
                    # Add user 2
                    target_user_name: str = self._get_account(id="2")["username"]
                    source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

                    # Add user 3
                    target_user_name: str = self._get_account(id="3")["username"]
                    source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

                elif project_name == self.config["projects"][2]["name"]:
                    """"""
                else:
                    raise NotImplementedError("Unknown project to update contributor on")

    def _set_project_properties(self):
        # 3. Set editors for user 1's projects
        source_user_conn: AEUserSession = self.get_account_conn(username=self._get_account(id="1")["username"])
        for project in self.projects:
            if project["record"]["owner"] == self._get_account(id="1")["username"]:
                project_name: str = project["record"]["name"]
                project_id: str = project["record"]["id"]
                logger.info(f"Setting default editor on project {project['record']['name']} for {project['record']['owner']}")

                if project_name == self.config["projects"][0]["name"]:
                    source_user_conn.project_patch(ident=project_id, editor="jupyterlab")  # jupyterlab, notebook, vscode
                elif project_name == self.config["projects"][1]["name"]:
                    source_user_conn.project_patch(ident=project_id, editor="vscode")  # jupyterlab, notebook, vscode
                elif project_name == self.config["projects"][2]["name"]:
                    source_user_conn.project_patch(ident=project_id, editor="notebook")  # jupyterlab, notebook, vscode
                else:
                    raise NotImplementedError("Unknown project to update default editor on")

    @staticmethod
    def gen_config(randomize: bool = True) -> dict:
        # load our fixtures
        with open(file="tests/fixtures/system/fixtures.json", mode="r", encoding="utf-8") as file:
            config: dict = json.load(file)

        # randomize!
        for account in config["accounts"]:
            prefix: str = "ae-system-test"
            account_id: str = str(uuid.uuid4())
            account["username"] = prefix + "-" + account_id
            account["email"] = account["username"] + "@localhost.local"
            account["firstname"] = account_id
            account["lastname"] = prefix
            account["password"] = str(uuid.uuid4())

        return config


if __name__ == "__main__":
    # Load env vars, - do NOT override previously defined ones
    load_dotenv(override=False)

    with SystemTestFixtureSuite(config=SystemTestFixtureSuite.gen_config(randomize=False)) as manager:
        # serialize to allow individual tests to operate (in other processes)
        with open(file="system-test-state.json", mode="w", encoding="utf-8") as file:
            file.write(str(manager))
        run()
