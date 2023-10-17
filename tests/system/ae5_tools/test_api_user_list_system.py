# from typing import Dict, List
#
# #####################################################
# # Test Cases For user_list
# #####################################################
#
#
# def test_user_list_has_realm_roles(admin_session):
#     # Test Case - User list contains realm roles
#
#     # Execute the test
#     user_list: List = admin_session.user_list()
#
#     # The live system will have more than 0 users
#     assert len(user_list) > 0
#
#     # Look for the admin account
#     account: Dict = [user for user in user_list if user["username"] == "anaconda-enterprise"][0]
#
#     # Ensure the new property is present
#     assert "realm_roles" in account
#
#     # Ensure the property has roles which would be present on the account
#     assert len(account["realm_roles"]) > 0
#
#     # Ensure the account has the expected roles assigned.
#     expected_roles: List[str] = [
#         "offline_access",
#         "ae-deployer",
#         "uma_authorization",
#         "ae-uploader",
#         "ae-admin",
#         "ae-creator",
#     ]
#     for role in expected_roles:
#         assert role in account["realm_roles"]
