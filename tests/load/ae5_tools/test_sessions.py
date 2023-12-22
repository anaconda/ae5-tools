import uuid

from dotenv import load_dotenv

from ae5_tools import AEUserSession
from tests.adsp.common.fixture_manager import FixtureManager

# Load env vars, - do NOT override previously defined ones
load_dotenv(override=False)


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


def test_sessions():
    with LoadTestFixtureSuite(config=LoadTestFixtureSuite.gen_config(size=100)) as manager:
        """"""
        print(str(manager))

        sessions: list[dict] = []
        records = [project["record"] for project in manager.projects]
        for record in records:
            owner: str = record["owner"]
            project_id: str = record["id"]
            account_conn: AEUserSession = manager.get_account_conn(username=owner)
            account_session = account_conn.session_start(ident=project_id, editor="jupyterlab", resource_profile="default", wait=False)
            sessions.append(account_session)
        print(sessions)
