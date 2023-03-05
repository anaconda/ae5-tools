import sys
import uuid
from typing import Dict
from unittest.mock import MagicMock
import datetime

import pytest


@pytest.fixture(scope="function")
def generate_raw_user_fixture() -> Dict:
    mock_user_id: str = str(uuid.uuid4())
    return {
        "id": mock_user_id,
        "name": "MOCK-USERNAME",
        "details": {},
        "userId": mock_user_id,
        "time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


#####################################################
# Test Cases For _get_user_realm_roles
#####################################################

def test_get_user_realm_roles(admin_session):
    test_cases = [
        # Test Case 1 - We get expected results
        {
            "user_uuid": str(uuid.uuid4()),
            "realm_roles": [{"name": "ae-admin", "id": str(uuid.uuid4())},
                            {"name": "ae-reader", "id": str(uuid.uuid4())}],
            "expected_results": ["ae-admin", "ae-reader"]
        },
        # Test Case 2 - No roles
        {
            "user_uuid": str(uuid.uuid4()),
            "realm_roles": [],
            "expected_results": []
        }
    ]

    for test_case in test_cases:
        # Set up test
        admin_session._get_paginated = MagicMock(return_value=test_case["realm_roles"])

        # Execute the test
        result = admin_session._get_user_realm_roles(user_uuid=test_case["user_uuid"])

        # Review results
        assert result == test_case["expected_results"]

        mock = admin_session._get_paginated
        mock.assert_called_once_with(f"users/{test_case['user_uuid']}/role-mappings/realm", limit=sys.maxsize, first=0)


#####################################################
# Test Cases For _merge_users_with_realm_roles
#####################################################

def test_merge_users_with_realm_roles(admin_session, generate_raw_user_fixture):
    test_cases = [
        # Test Case 1 - Empty Roles
        {
            "realm_roles": [],
        },
        # Test Case 2 - Matching Roles For A Single User
        {
            "realm_roles": ["ae-admin", "ae-reader"],
        },
    ]

    for test_case in test_cases:
        test_case["users"] = [generate_raw_user_fixture]
        test_case["expected_results"] = [{
            **test_case["users"][0],
            "realm_roles": test_case["realm_roles"]
        }]

        # Set up test
        admin_session._get_user_realm_roles = MagicMock(return_value=test_case["realm_roles"])

        # Execute the test
        result = admin_session._merge_users_with_realm_roles(users=test_case["users"])

        # Review results
        assert result == test_case["expected_results"]

        mock = admin_session._get_user_realm_roles
        mock.assert_called_once_with(user_uuid=test_case["users"][0]["id"])


#####################################################
# Test Cases For user_list
#####################################################

def test_user_list_with_no_users(admin_session):
    test_cases = [
        # Test Case 1 - No Users
        {
            "users": [],
            "mapped_users": [],
            "expected_results": []
        },
    ]

    for test_case in test_cases:
        # Set up test
        admin_session._get_paginated = MagicMock(return_value=test_case["users"])
        admin_session._merge_users_with_realm_roles = MagicMock(return_value=test_case["mapped_users"])

        # Execute the test
        result = admin_session.user_list()

        # Review results
        assert result == test_case["expected_results"]


def test_user_list(admin_session, generate_raw_user_fixture):
    test_cases = [
        # Test Case 1 - User does not have any roles
        {
            "realm_roles": []
        },
        # Test 2 - User belongs to a role
        {
            "realm_roles": ["ae-admin"],
        }
    ]

    for test_case in test_cases:
        test_case["users"] = [generate_raw_user_fixture]
        test_case["mapped_users"] = [{
            **test_case["users"][0],
            "realm_roles": test_case["realm_roles"]
        }]
        test_case["expected_results"] = [{
            **test_case["users"][0],
            "realm_roles": test_case["realm_roles"],
            "_record_type": "user",
            "lastLogin": test_case["users"][0]["time"]
        }]

        # Set up test
        admin_session._get_paginated = MagicMock(return_value=test_case["users"])
        admin_session._merge_users_with_realm_roles = MagicMock(return_value=test_case["mapped_users"])

        # Execute the test
        result = admin_session.user_list()

        # Review results
        assert result == test_case["expected_results"]
