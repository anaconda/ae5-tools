import datetime
import os
import sys
import uuid
from typing import Dict, List
from unittest.mock import MagicMock

import pytest
from ae5_tools.api import AEAdminSession


@pytest.fixture(scope="function")
def get_token_fixture():
    return {
        "access_token": str(uuid.uuid4()),
        "refresh_token": str(uuid.uuid4()),
    }


@pytest.fixture(scope="function")
def admin_session(get_token_fixture):
    admin_session = AEAdminSession(
        hostname="MOCK-HOSTNAME", username="MOCK-AE-USERNAME", password="MOCK-AE-USER-PASSWORD"
    )
    admin_session._load = MagicMock()
    admin_session._sdata = get_token_fixture
    return admin_session


@pytest.fixture(scope="function")
def generate_raw_user_fixture() -> Dict:
    mock_user_id: str = str(uuid.uuid4())
    return {
        "id": mock_user_id,
        "name": "MOCK-USERNAME",
        "details": {},
        "userId": mock_user_id,
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


#####################################################
# Test Cases For _get_role_users
#####################################################
def test_get_role_users(admin_session):
    test_cases: List[Dict] = [
        # Scenario 1 - No users are mapped to the role
        {"role_users": []},
        # Scenario 2 - A single mapped user
        {"role_users": [{"username": "mock-user"}]},
    ]

    for test_case in test_cases:
        # Set up test
        admin_session._get_paginated = MagicMock(return_value=test_case["role_users"])

        # Execute the test
        role_users: List[Dict] = admin_session._get_role_users(role_name="MOCK-ROLE-NAME")

        # Review the results
        assert role_users == test_case["role_users"]

        mock = admin_session._get_paginated
        mock.assert_called_once_with(path="roles/MOCK-ROLE-NAME/users", first=0, max=sys.maxsize)


#####################################################
# Test Cases For _get_realm_roles
#####################################################
def test_get_realm_roles(admin_session):
    # Set up test
    admin_session._get_paginated = MagicMock()

    # Execute the test
    admin_session._get_realm_roles()

    mock = admin_session._get_paginated
    mock.assert_called_once_with(path="roles", first=0, max=sys.maxsize)


#####################################################
# Test Cases For _get_user_realm_roles
#####################################################
def test_get_user_realm_roles_from_map(admin_session, generate_raw_user_fixture):
    test_cases: List[Dict] = [
        # Scenario 1 - Nothing to generate
        {"user": {}, "role_maps": {}, "expected_realm_roles": []},
        # Scenario 2 - No roles assigned to user
        {"user": generate_raw_user_fixture, "role_maps": {}, "expected_realm_roles": []},
        # Scenario 3 - Found a mapped role
        {
            "user": generate_raw_user_fixture,
            "role_maps": {"ae-admin": [generate_raw_user_fixture]},
            "expected_realm_roles": ["ae-admin"],
        },
    ]

    for test_case in test_cases:
        # Execute the test
        user_realm_roles: List[str] = admin_session._get_user_realm_roles(
            user=test_case["user"], role_maps=test_case["role_maps"]
        )

        # Review the results
        assert user_realm_roles == test_case["expected_realm_roles"]


#####################################################
# Test Cases For _build_realm_role_user_map
#####################################################
def test_build_realm_role_user_map(admin_session, generate_raw_user_fixture):
    test_cases: List[Dict] = [
        # Scenario 1 - No realm roles
        {"realm_roles": [], "role_users": [], "expected_role_maps": {}},
        # Scenario 2 - Role exists, but no users are mapped to it.
        {"realm_roles": [{"name": "ae-admin"}], "role_users": [], "expected_role_maps": {"ae-admin": []}},
        # Scenario 3 - Role exists, and a user is mapped
        {
            "realm_roles": [{"name": "ae-admin"}],
            "role_users": [generate_raw_user_fixture],
            "expected_role_maps": {"ae-admin": [generate_raw_user_fixture]},
        },
    ]

    for test_case in test_cases:
        # Set up the test
        admin_session._get_realm_roles = MagicMock(return_value=test_case["realm_roles"])
        admin_session._get_role_users = MagicMock(return_value=test_case["role_users"])

        # Execute the test
        role_maps = admin_session._build_realm_role_user_map()

        # Review the result
        assert role_maps == test_case["expected_role_maps"]

        mock = admin_session._get_realm_roles
        mock.assert_called_once()

        mock = admin_session._get_role_users
        for role in test_case["realm_roles"]:
            mock.assert_called_with(role_name=role["name"])


def test_build_realm_role_user_map_multiple_returns(admin_session, generate_raw_user_fixture):
    # Scenario - Multiple roles and mappings.
    test_case = {
        "realm_roles": [{"name": "ae-admin"}, {"name": "ae-reader"}, {"name": "fake-role"}],
        "role_users_1": [generate_raw_user_fixture],
        "role_users_2": [generate_raw_user_fixture, generate_raw_user_fixture],
        "role_users_3": [],
        "expected_role_maps": {
            "ae-admin": [generate_raw_user_fixture],
            "ae-reader": [generate_raw_user_fixture, generate_raw_user_fixture],
            "fake-role": [],
        },
    }

    # Set up the test
    admin_session._get_realm_roles = MagicMock(return_value=test_case["realm_roles"])
    admin_session._get_role_users = MagicMock(
        side_effect=[test_case["role_users_1"], test_case["role_users_2"], test_case["role_users_3"]]
    )

    # Execute the test
    role_maps = admin_session._build_realm_role_user_map()

    # Review the result
    assert role_maps == test_case["expected_role_maps"]


#####################################################
# Test Cases For _merge_users_with_realm_roles
#####################################################


def test_merge_users_with_realm_roles(admin_session, generate_raw_user_fixture):
    test_cases = [
        # Test Case 1 - Empty Roles
        {"realm_roles": [], "role_maps": {"ae-admin": []}},
        # Test Case 2 - Matching Roles For A Single User
        {"realm_roles": ["ae-admin", "ae-reader"], "role_maps": {}},
    ]

    for test_case in test_cases:
        test_case["users"] = [generate_raw_user_fixture]
        test_case["expected_results"] = [{**test_case["users"][0], "realm_roles": test_case["realm_roles"]}]

        # Set up test
        admin_session._get_user_realm_roles = MagicMock(return_value=test_case["realm_roles"])

        # Execute the test
        result = admin_session._merge_users_with_realm_roles(users=test_case["users"], role_maps=test_case["role_maps"])

        # Review results
        assert result == test_case["expected_results"]

        mock = admin_session._get_user_realm_roles
        mock.assert_called_once_with(user=test_case["users"][0], role_maps=test_case["role_maps"])


#####################################################
# Test Cases For user_list
#####################################################


def test_user_list_roles(admin_session, generate_raw_user_fixture):
    test_cases = [
        # Test Case 1 - No Users
        {
            "role_maps": {
                "ae-admin": [generate_raw_user_fixture],
                "ae-reader": [generate_raw_user_fixture, generate_raw_user_fixture],
                "fake-role": [],
            },
            "users": [],
            "events": [],
            "merged_users": [],
        },
        # Test Case 2 - Users with mapped roles are returned
        {
            "role_maps": {
                "ae-admin": [generate_raw_user_fixture],
                "ae-reader": [generate_raw_user_fixture, generate_raw_user_fixture],
                "fake-role": [],
            },
            "users": [generate_raw_user_fixture],
            "events": [],
            "merged_users": [{**generate_raw_user_fixture, "realm_roles": ["ae-admin", "ae-reader"]}],
        },
    ]

    for test_case in test_cases:
        # Set up test
        admin_session._get_paginated = MagicMock(side_effect=[test_case["users"], test_case["events"]])
        admin_session._build_realm_role_user_map = MagicMock(return_value=test_case["role_maps"])
        admin_session._merge_users_with_realm_roles = MagicMock(return_value=test_case["merged_users"])

        # Execute the test
        result = admin_session.user_list()

        # Review results
        assert result == test_case["merged_users"]

        mock = admin_session._merge_users_with_realm_roles
        mock.assert_called_once_with(users=(test_case["users"] + test_case["events"]), role_maps=test_case["role_maps"])
