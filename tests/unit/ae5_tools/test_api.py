import sys
import unittest
import uuid
from typing import Dict
from unittest.mock import MagicMock
import datetime

from ae5_tools.api import AEAdminSession


class TestAPI(unittest.TestCase):

    def setUp(self):
        self.adm_session = AEAdminSession(hostname="MOCK-HOSTNAME", username="MOCK-USERNAME", password="")

    #####################################################
    # _get_user_realm_roles Tests
    #####################################################

    def test_get_user_realm_roles(self):
        #####################################################
        # Test Cases For _get_user_realm_roles
        #####################################################

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
            self.adm_session._get_paginated = MagicMock(return_value=test_case["realm_roles"])

            # Execute the test
            result = self.adm_session._get_user_realm_roles(user_uuid=test_case["user_uuid"])

            # Review results
            self.assertEqual(result, test_case["expected_results"])

            mock = self.adm_session._get_paginated
            mock.assert_called_once_with(f"users/{test_case['user_uuid']}/role-mappings/realm", limit=sys.maxsize, first=0)

    #####################################################
    # _merge_users_with_realm_roles Tests
    #####################################################

    def test_merge_users_with_realm_roles(self):
        #####################################################
        # Test Cases For _merge_users_with_realm_roles
        #####################################################

        mock_user_uuid: str = str(uuid.uuid4())
        mock_realm_roles: Dict[str] = ["ae-admin", "ae-reader"]
        test_cases = [
            # Test Case 1 - Empty Roles
            {
                "realm_roles": [],
                "users": [{
                    "id": mock_user_uuid,
                    "name": "MOCK-USERNAME"
                }],
                "expected_results": [{
                    "id": mock_user_uuid,
                    "name": "MOCK-USERNAME",
                    "realm_roles": []
                }]
            },
            # Test Case 2 - Matching Roles For A Single User
            {
                "realm_roles": mock_realm_roles,
                "users": [{
                    "id": mock_user_uuid,
                    "name": "MOCK-USERNAME"
                }],
                "expected_results": [{
                    "id": mock_user_uuid,
                    "name": "MOCK-USERNAME",
                    "realm_roles": mock_realm_roles
                }]
            },
        ]

        for test_case in test_cases:
            # Set up test
            self.adm_session._get_user_realm_roles = MagicMock(return_value=test_case["realm_roles"])

            # Execute the test
            result = self.adm_session._merge_users_with_realm_roles(users=test_case["users"])

            # Review results
            self.assertEqual(result, test_case["expected_results"])

            mock = self.adm_session._get_user_realm_roles
            mock.assert_called_once_with(user_uuid=mock_user_uuid)
