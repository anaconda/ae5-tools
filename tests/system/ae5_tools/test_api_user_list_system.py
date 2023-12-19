#####################################################
# Test Cases For user_list
#####################################################


def test_user_list_has_realm_roles(admin_session):
    # Test Case - User list contains realm roles

    # Execute the test
    user_list: list = admin_session.user_list()

    # The live system will have more than 0 users
    assert len(user_list) > 0

    # Look for the admin account
    account: dict = [user for user in user_list if user["username"] == "anaconda-enterprise"][0]

    # Ensure the new property is present
    assert "realm_roles" in account

    # Ensure the property has roles which would be present on the account
    assert len(account["realm_roles"]) > 0

    # Ensure the account has the expected roles assigned.

    # Roles
    expected_roles: list[str] = [
        "offline_access",
        "uma_authorization",
    ]

    # Groups
    expected_groups: list[str] = ["admins", "developers", "everyone"]

    for role in expected_roles:
        assert role in account["realm_roles"]
    for group in expected_groups:
        assert group in account["realm_groups"]
