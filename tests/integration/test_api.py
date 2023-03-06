import json
import subprocess
import shlex
from typing import Dict, List
import uuid
import pytest

from tests.integration.mock.ae5 import AE5MockClient


def shell_out(cmd: str) -> tuple[str, str, int]:
    args = shlex.split(cmd)
    proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    try:
        outs, errs = proc.communicate()
    except subprocess.TimeoutExpired:
        proc.kill()
        outs, errs = proc.communicate()
    return outs, errs.decode(encoding="utf-8"), proc.returncode


@pytest.fixture(scope="function")
def get_token_fixture():
    return {
        "access_token": str(uuid.uuid4()),
        "refresh_token": str(uuid.uuid4()),
    }


@pytest.fixture(scope="function")
def get_user_fixture():
    return {
        "username": "mock-ae-username",
        "firstName": "MOCK",
        "lastName": "USER",
        "lastLogin": 1677711863478,
        "email": "mock-ae-username@localhost.local",
        "id": str(uuid.uuid4()),
        "createdTimestamp": 1677711863478,
        "enabled": True,
        "totp": False,
        "emailVerified": True,
        "disableableCredentialTypes": [],
        "requiredActions": [],
        "notBefore": 0,
        "access": {
            "manageGroupMembership": True,
            "view": True,
            "mapRoles": True,
            "impersonate": True,
            "manage": True
        },
        "_record_type": "user"
    }


#####################################################
# Test Cases For `user list` with roles
#####################################################

def test_user_list_roles(get_token_fixture, get_user_fixture):
    # Define the test scenarios
    test_cases: List[Dict] = [
        # Scenario 1 - User is does not have any roles.
        {
            "state": {
                "get_token": {
                    "calls": [],
                    "responses": [get_token_fixture],
                },
                "get_users": {
                    "calls": [],
                    "responses": [[get_user_fixture]]
                },
                "get_events": {
                    "calls": [],
                    "responses": [[]]
                },
                "get_realm_roles": {
                    "calls": [],
                    "responses": [[]]
                }
            },
            "command": "python -m ae5_tools.cli.main user list --format json",
            "expected_results": [[]]
        },
        # Scenario 2 - User has two roles assigned
        {
            "state": {
                "get_token": {
                    "calls": [],
                    "responses": [get_token_fixture],
                },
                "get_users": {
                    "calls": [],
                    "responses": [[get_user_fixture]]
                },
                "get_events": {
                    "calls": [],
                    "responses": [[]]
                },
                "get_realm_roles": {
                    "calls": [],
                    "responses": [[
                        {
                            "name": "ae-admin"
                        },
                        {
                            "name": "ae-reader"
                        },
                    ]]
                }
            },
            "command": "python -m ae5_tools.cli.main user list --format json",
            "expected_results": [["ae-admin", "ae-reader"]]
        }
    ]

    for test_case in test_cases:
        # Reset mock state
        AE5MockClient.reset_mock_state()

        # Set up the test (configure the mock)
        AE5MockClient.set_mock_state(mock_state=test_case["state"])

        # Execute the test
        outs, errs, returncode = shell_out(cmd=test_case["command"])

        # Review the outcome
        outs_lines: str = outs.decode("utf-8")
        out_data: Dict = json.loads(outs_lines)
        # print(out_data)

        # Ensure realm roles is present and populated correctly.
        assert "realm_roles" in out_data[0]
        assert out_data[0]["realm_roles"] == test_case["expected_results"][0]

        # Review calls make to AE5.
        mock_state: Dict = AE5MockClient.get_mock_state()

        # Ensure asked for roles only once
        assert len(mock_state["get_realm_roles"]["calls"]) == 1

        # Assert that we asked for roles for the user fixture
        assert mock_state["get_realm_roles"]["calls"][0]["user_uuid"] == get_user_fixture["id"]
