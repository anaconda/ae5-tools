from typing import List
from unittest.mock import MagicMock

from ae5_tools.api import AEUserSession, AEException
import uuid
import pytest


@pytest.fixture(scope="function")
def get_token_fixture():
    return {
        "access_token": str(uuid.uuid4()),
        "refresh_token": str(uuid.uuid4()),
    }


@pytest.fixture(scope="function")
def user_session(get_token_fixture):
    user_session = AEUserSession(
        hostname="MOCK-HOSTNAME", username="MOCK-AE-USERNAME", password="MOCK-AE-USER-PASSWORD"
    )
    return user_session

#####################################################
# Test Cases For secret_add
#####################################################

def test_secret_add(user_session, monkeypatch):
    # Set up test
    mock_key: str = "MOCK-KEY"
    mock_value: str = "MOCK-VALUE"

    mock_post: MagicMock = MagicMock()

    monkeypatch.setattr(AEUserSession, "_post", mock_post)

    # Execute the test
    user_session.secret_add(key=mock_key, value=mock_value)

    # Review the results
    print(mock_post.assert_called_once_with("credentials/user", json={"key": "MOCK-KEY", "value": "MOCK-VALUE"}))


#####################################################
# Test Cases For secret_delete
#####################################################

def test_secret_delete(user_session, monkeypatch):
    # Set up test
    mock_key: str = "MOCK-KEY"
    mock_delete: MagicMock = MagicMock()
    secret_list_mock: MagicMock = MagicMock(return_value=[{"secrets": [mock_key]}])
    monkeypatch.setattr(AEUserSession, "secret_list", secret_list_mock)
    monkeypatch.setattr(AEUserSession, "_delete", mock_delete)

    # Execute the test
    user_session.secret_delete(key=mock_key)

    # Review the results
    mock_delete.assert_called_once_with("credentials/user/MOCK-KEY")


def test_secret_delete_with_missing_secret(user_session, monkeypatch):
    # Set up test
    mock_key: str = "MOCK-KEY"
    mock_delete: MagicMock = MagicMock()
    secret_list_mock: MagicMock = MagicMock(return_value=[{"secrets": []}])
    monkeypatch.setattr(AEUserSession, "secret_list", secret_list_mock)

    # Execute the test
    with pytest.raises(AEException) as context:
        user_session.secret_delete(key=mock_key)

    # Review the result
    assert str(context.value) == "User secret 'MOCK-KEY' was not found and cannot be deleted."


#####################################################
# Test Cases For secret_list
#####################################################

def test_secret_list(user_session, monkeypatch):
    # Set up the test
    mock_key: str = "MOCK-KEY"
    mock_get: MagicMock = MagicMock(return_value={"data": [mock_key]})
    monkeypatch.setattr(AEUserSession, "_get", mock_get)

    # Execute the test
    secrets: List[str] = user_session.secret_list()

    # Review the result
    assert secrets == [{"secrets": [mock_key]}]


def test_secret_list_gracefully_fails(user_session, monkeypatch):
    # Set up the test
    mock_key: str = "MOCK-KEY"
    mock_get: MagicMock = MagicMock(return_value={})
    monkeypatch.setattr(AEUserSession, "_get", mock_get)

    # Execute the test
    with pytest.raises(AEException) as context:
        user_session.secret_list()

    # Review the result
    assert str(context.value) == "Failed to retrieve user secrets."

