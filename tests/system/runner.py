""" """

from __future__ import annotations

import json
import logging

from dotenv import load_dotenv

from ae5_tools import AEUserSession
from tests.adsp.common.abstract_fixture_suite import AbstractFixtureSuite
from tests.adsp.common.fixture_manager import FixtureManager
from tests.adsp.common.utils import _process_launch_wait

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    shell_out_cmd: str = "python -m pytest --cov=ae5_tools --show-capture=all -rP tests/system --cov-append --cov-report=xml -vv"
    _process_launch_wait(shell_out_cmd=shell_out_cmd)


class SystemTestFixtureSuite(AbstractFixtureSuite):
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

        # Create service accounts (and connections)
        self.manager.create_fixture_accounts(accounts=self.config["service_accounts"], force=self.config["force"])
        self.manager.create_fixture_connections()

        # 1. Each user gets all three projects.
        for account in self.config["service_accounts"]:
            for proj in self.config["projects"]:
                self.manager.upload_fixture_project(proj_params=proj, owner=account["username"], force=self.config["force"])

        # 2. Build our relationships.
        logger.info("Building project / account relationships")

        # User 3 shares projects 1 & 2 with User 1
        source_user_conn: AEUserSession = self.manager.get_account_conn(username=self.config["service_accounts"][2]["username"])
        target_user_name: str = self.config["service_accounts"][0]["username"]

        for project in self.manager.projects:
            if project["record"]["owner"] == self.config["service_accounts"][2]["username"] and project["record"]["name"] in [
                "testproj1",
                "testproj2",
            ]:
                project_id: str = project["record"]["id"]
                response = source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

        # User 1 shares projects to different numbers of users
        source_user_conn: AEUserSession = self.manager.get_account_conn(username=self.config["service_accounts"][0]["username"])
        for project in self.manager.projects:
            if project["record"]["owner"] == self.config["service_accounts"][0]["username"]:
                project_name: str = project["record"]["name"]
                project_id: str = project["record"]["id"]
                logger.info(f"Configuring sharing on project {project['record']['name']} for {project['record']['owner']}")

                if project_name == self.config["projects"][0]["name"]:
                    # Add user 2
                    target_user_name: str = self.config["service_accounts"][1]["username"]
                    source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)
                elif project_name == self.config["projects"][1]["name"]:
                    # Add user 2
                    target_user_name: str = self.config["service_accounts"][1]["username"]
                    source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

                    # Add user 3
                    target_user_name: str = self.config["service_accounts"][2]["username"]
                    source_user_conn.project_collaborator_add(ident=project_id, userid=target_user_name)

                elif project_name == self.config["projects"][2]["name"]:
                    """"""
                else:
                    raise NotImplementedError("Unknown project to update contributor on")

        # 3. Set editors for user 1's projects
        source_user_conn: AEUserSession = self.manager.get_account_conn(username=self.config["service_accounts"][0]["username"])
        for project in self.manager.projects:
            if project["record"]["owner"] == self.config["service_accounts"][0]["username"]:
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


if __name__ == "__main__":
    # Load env vars, - do NOT override previously defined ones
    load_dotenv(override=False)

    # load our fixtures
    with open(file="tests/fixtures/system/fixtures.json", mode="r", encoding="utf-8") as file:
        config: dict = json.load(file)

    with SystemTestFixtureSuite(config=config, manager=FixtureManager()) as fixtures:
        # Execute the test runner
        run()
