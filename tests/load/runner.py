""" """

from __future__ import annotations

import logging
import uuid

from dotenv import load_dotenv

from ae5_tools import demand_env_var_as_bool, get_env_var
from tests.adsp.common.fixture_manager import FixtureManager
from tests.adsp.common.utils import _process_launch_wait

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def run() -> None:
    shell_out_cmd: str = "python -m pytest --cov=ae5_tools --show-capture=all -rP tests/load/ae5_tools --cov-append --cov-report=xml -vv"

    if get_env_var(name="CI") and demand_env_var_as_bool(name="CI"):
        shell_out_cmd += " --ci-skip"

    _process_launch_wait(shell_out_cmd=shell_out_cmd)


class LoadTestFixtureSuite(FixtureManager):
    def _setup(self) -> None:
        # Create Fixtures

        self._create_service_accounts()
        self._upload_projects()

    def _create_service_accounts(self):
        # Create service accounts (and connections)
        self.create_fixture_accounts(accounts=self.config["accounts"], force=self.config["force"])
        self.create_fixture_connections()

    def _upload_projects(self):
        # 1. Each user gets a project.
        for account, proj in zip(self.config["accounts"], self.config["projects"]):
            self.upload_fixture_project(proj_params=proj, owner=account["username"], force=self.config["force"])

    @staticmethod
    def gen_config(size: int = 1) -> dict:
        # load our fixtures
        config: dict = {
            "force": True,
            "teardown": True,
            "accounts": [],
            "projects": [],
        }

        prefix: str = "ae-load-test"
        for i in range(size):
            account: dict = {}
            account["id"] = str(i + 1)
            account["username"] = prefix + "-account-" + account["id"]
            account["email"] = account["username"] + "@localhost.local"
            account["firstname"] = prefix + "-account"
            account["lastname"] = account["id"]
            account["password"] = str(uuid.uuid4())
            config["accounts"].append(account)

            project: dict = {"name": prefix + "-" + str(i + 1), "artifact": "tests/fixtures/system/testproj1.tar.gz", "tag": "0.1.0"}
            config["projects"].append(project)

        return config


if __name__ == "__main__":
    # Load env vars, - do NOT override previously defined ones
    load_dotenv(override=False)

    with LoadTestFixtureSuite(config=LoadTestFixtureSuite.gen_config(size=2)) as manager:
        # serialize to allow individual tests to operate (in other processes)
        with open(file="load-test-state.json", mode="w", encoding="utf-8") as file:
            file.write(str(manager))
            run()
